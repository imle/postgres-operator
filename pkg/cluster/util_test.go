package cluster

import (
	"context"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	acidv1 "github.com/zalando/postgres-operator/pkg/apis/acid.zalan.do/v1"
	fakeacidv1 "github.com/zalando/postgres-operator/pkg/generated/clientset/versioned/fake"
	"github.com/zalando/postgres-operator/pkg/util/config"
	"github.com/zalando/postgres-operator/pkg/util/k8sutil"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8sFake "k8s.io/client-go/kubernetes/fake"
)

func newFakeK8sAnnotationsClient() (k8sutil.KubernetesClient, *k8sFake.Clientset) {
	clientSet := k8sFake.NewSimpleClientset()
	acidClientSet := fakeacidv1.NewSimpleClientset()

	return k8sutil.KubernetesClient{
		PodDisruptionBudgetsGetter:   clientSet.PolicyV1(),
		ServicesGetter:               clientSet.CoreV1(),
		StatefulSetsGetter:           clientSet.AppsV1(),
		PostgresqlsGetter:            acidClientSet.AcidV1(),
		PersistentVolumeClaimsGetter: clientSet.CoreV1(),
		PersistentVolumesGetter:      clientSet.CoreV1(),
		EndpointsGetter:              clientSet.CoreV1(),
		PodsGetter:                   clientSet.CoreV1(),
		DeploymentsGetter:            clientSet.AppsV1(),
	}, clientSet
}

func newInheritedAnnotationsCluster(client k8sutil.KubernetesClient) (*Cluster, error) {
	annotationValue := "acid"
	role := Master

	pg := acidv1.Postgresql{
		ObjectMeta: metav1.ObjectMeta{
			Name: clusterName,
			Annotations: map[string]string{
				"owned-by": annotationValue,
				"foo":      "bar",
			},
		},
		Spec: acidv1.PostgresSpec{
			EnableReplicaConnectionPooler: boolToPointer(true),
			Volume: acidv1.Volume{
				Size: "1Gi",
			},
		},
	}

	cluster := New(
		Config{
			OpConfig: config.Config{
				ConnectionPooler: config.ConnectionPooler{
					ConnectionPoolerDefaultCPURequest:    "100m",
					ConnectionPoolerDefaultCPULimit:      "100m",
					ConnectionPoolerDefaultMemoryRequest: "100Mi",
					ConnectionPoolerDefaultMemoryLimit:   "100Mi",
					NumberOfInstances:                    k8sutil.Int32ToPointer(1),
				},
				PodManagementPolicy: "ordered_ready",
				Resources: config.Resources{
					ClusterLabels:         map[string]string{"application": "spilo"},
					ClusterNameLabel:      "cluster-name",
					DefaultCPURequest:     "300m",
					DefaultCPULimit:       "300m",
					DefaultMemoryRequest:  "300Mi",
					DefaultMemoryLimit:    "300Mi",
					InheritedAnnotations:  []string{"owned-by"},
					PodRoleLabel:          "spilo-role",
					ResourceCheckInterval: time.Duration(testResourceCheckInterval),
					ResourceCheckTimeout:  time.Duration(testResourceCheckTimeout),
				},
			},
		}, client, pg, logger, eventRecorder)
	cluster.Name = clusterName
	cluster.Namespace = namespace
	cluster.ConnectionPooler = map[PostgresRole]*ConnectionPoolerObjects{}
	cluster.ConnectionPooler[role] = &ConnectionPoolerObjects{
		Name:        cluster.connectionPoolerName(role),
		ClusterName: cluster.Name,
		Namespace:   cluster.Namespace,
		Role:        role,
	}
	_, err := cluster.createStatefulSet()
	if err != nil {
		return nil, err
	}
	_, err = cluster.createService(Master)
	if err != nil {
		return nil, err
	}
	_, err = cluster.createPodDisruptionBudget()
	if err != nil {
		return nil, err
	}
	_, err = cluster.generateConnectionPoolerDeployment(cluster.ConnectionPooler[role])
	if err != nil {
		return nil, err
	}
	_, err = cluster.syncConnectionPoolerWorker(nil, &pg, Master)
	if err != nil {
		return nil, err
	}
	pvcList := CreatePVCs(namespace, clusterName, cluster.labelsSet(false), 2, "1Gi")
	for _, pvc := range pvcList.Items {
		cluster.KubeClient.PersistentVolumeClaims(namespace).Create(context.TODO(), &pvc, metav1.CreateOptions{})
	}

	return cluster, nil
}

func TestInheritedAnnotations(t *testing.T) {
	client, _ := newFakeK8sAnnotationsClient()
	cluster, err := newInheritedAnnotationsCluster(client)
	assert.NoError(t, err)

	// test annotationsSet function
	inheritedAnnotations := cluster.annotationsSet(nil)
	assert.Equal(t, len(inheritedAnnotations), 1)

	filterLabels := cluster.labelsSet(false)
	listOptions := metav1.ListOptions{
		LabelSelector: filterLabels.String(),
	}

	// helper functions
	checkInheritedAnnotations := func(actual map[string]string, objName string, objType string) error {
		if !(reflect.DeepEqual(actual, inheritedAnnotations)) {
			return fmt.Errorf("%s %v not inherited annotations %#v, got %#v", objType, objName, inheritedAnnotations, actual)
		}
		return nil
	}

	checkAnnotationsRemoved := func(actual map[string]string, objName string, objType string) error {
		if len(actual) > 0 {
			return fmt.Errorf("%s %v should not have any annotations, got %#v", objType, objName, actual)
		}
		return nil
	}

	// statefulset annotations
	checkSts := func(isRemoved bool) error {
		stsList, err := client.StatefulSets(namespace).List(context.TODO(), listOptions)
		assert.NoError(t, err)
		checkFunc := checkInheritedAnnotations
		if isRemoved {
			checkFunc = checkAnnotationsRemoved
		}

		for _, sts := range stsList.Items {
			if err := checkFunc(sts.ObjectMeta.Annotations, sts.ObjectMeta.Name, "StatefulSet"); err != nil {
				return err
			}

			// pod template
			if err := checkFunc(sts.Spec.Template.ObjectMeta.Annotations, sts.ObjectMeta.Name, "StatefulSet pod template"); err != nil {
				return err
			}

			// pvc template
			if err := checkFunc(sts.Spec.VolumeClaimTemplates[0].Annotations, sts.ObjectMeta.Name, "StatefulSet pvc template"); err != nil {
				return err
			}
		}
		return nil
	}

	// service annotations
	checkSvc := func(isRemoved bool) error {
		svcList, err := client.Services(namespace).List(context.TODO(), listOptions)
		assert.NoError(t, err)
		checkFunc := checkInheritedAnnotations
		if isRemoved {
			checkFunc = checkAnnotationsRemoved
		}

		for _, svc := range svcList.Items {
			if err := checkFunc(svc.ObjectMeta.Annotations, svc.ObjectMeta.Name, "Service"); err != nil {
				return err
			}
		}
		return nil
	}

	// pod disruption budget annotations
	checkPdb := func(isRemoved bool) error {
		pdbList, err := client.PodDisruptionBudgets(namespace).List(context.TODO(), listOptions)
		assert.NoError(t, err)
		checkFunc := checkInheritedAnnotations
		if isRemoved {
			checkFunc = checkAnnotationsRemoved
		}

		for _, pdb := range pdbList.Items {
			if err := checkFunc(pdb.ObjectMeta.Annotations, pdb.ObjectMeta.Name, "Pod Disruption Budget"); err != nil {
				return err
			}
		}
		return nil
	}

	// PVC annotations
	checkPvc := func(isRemoved bool) error {
		pvcs, err := cluster.listPersistentVolumeClaims()
		assert.NoError(t, err)
		checkFunc := checkInheritedAnnotations
		if isRemoved {
			checkFunc = checkAnnotationsRemoved
		}

		for _, pvc := range pvcs {
			if err := checkFunc(pvc.ObjectMeta.Annotations, pvc.ObjectMeta.Name, "Volume claim"); err != nil {
				return err
			}
		}
		return nil
	}

	checkPooler := func(isRemoved bool) error {
		deploy, err := client.Deployments(namespace).Get(context.TODO(), cluster.connectionPoolerName(Master), metav1.GetOptions{})
		assert.NoError(t, err)
		checkFunc := checkInheritedAnnotations
		if isRemoved {
			checkFunc = checkAnnotationsRemoved
		}

		if err := checkFunc(deploy.ObjectMeta.Annotations, deploy.ObjectMeta.Name, "Deployment"); err != nil {
			return err
		}
		return nil
	}

	// 1. Check initial state
	err = checkSts(false)
	assert.NoError(t, err)

	err = checkSvc(false)
	assert.NoError(t, err)

	err = checkPdb(false)
	assert.NoError(t, err)

	// pooler deployment annotations
	_, err = cluster.syncConnectionPoolerWorker(nil, &cluster.Postgresql, Master)
	assert.NoError(t, err)
	err = checkPooler(false)
	assert.NoError(t, err)

	cluster.syncVolumes()
	err = checkPvc(false)
	assert.NoError(t, err)

	// 2. Check annotation value change
	cluster.ObjectMeta.Annotations["owned-by"] = "foo"
	inheritedAnnotations = cluster.annotationsSet(nil)

	cluster.syncStatefulSet()
	err = checkSts(false)
	assert.NoError(t, err)

	cluster.syncServices()
	err = checkSvc(false)
	assert.NoError(t, err)

	cluster.syncPodDisruptionBudget(false)
	err = checkPdb(false)
	assert.NoError(t, err)

	_, err = cluster.syncConnectionPoolerWorker(nil, &cluster.Postgresql, Master)
	assert.NoError(t, err)
	err = checkPooler(false)
	assert.NoError(t, err)

	// PVC annotations + new PVC
	cluster.KubeClient.PersistentVolumeClaims(namespace).Create(context.TODO(), &CreatePVCs(namespace, clusterName+"-2", filterLabels, 1, "1Gi").Items[0], metav1.CreateOptions{})
	cluster.syncVolumes()
	err = checkPvc(false)
	assert.NoError(t, err)

	// 3. Check removal of an inherited annotation
	delete(cluster.ObjectMeta.Annotations, "owned-by")

	cluster.syncStatefulSet()
	err = checkSts(true)
	assert.NoError(t, err)

	cluster.syncServices()
	err = checkSvc(true)
	assert.NoError(t, err)

	cluster.syncPodDisruptionBudget(false)
	err = checkPdb(true)
	assert.NoError(t, err)

	cluster.syncVolumes()
	err = checkPvc(true)
	assert.NoError(t, err)
}

func Test_trimCronjobName(t *testing.T) {
	type args struct {
		name string
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			name: "short name",
			args: args{
				name: "short-name",
			},
			want: "short-name",
		},
		{
			name: "long name",
			args: args{
				name: "very-very-very-very-very-very-very-very-very-long-db-name",
			},
			want: "very-very-very-very-very-very-very-very-very-long-db",
		},
		{
			name: "long name should not end with dash",
			args: args{
				name: "very-very-very-very-very-very-very-very-very-----------long-db-name",
			},
			want: "very-very-very-very-very-very-very-very-very",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := trimCronjobName(tt.args.name); got != tt.want {
				t.Errorf("trimCronjobName() = %v, want %v", got, tt.want)
			}
		})
	}
}

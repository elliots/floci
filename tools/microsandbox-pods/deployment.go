package main

import "net/http"

type deploymentView struct {
	Namespace   string          `json:"namespace"`
	Name        string          `json:"name"`
	UID         string          `json:"uid"`
	Generation  int64           `json:"generation"`
	Replicas    int32           `json:"replicas"`
	ClusterName string          `json:"clusterName"`
	Pods        []lifecycleView `json:"pods"`
}

func (p *provider) deployment(w http.ResponseWriter, r *http.Request) {
	namespace, name := r.PathValue("namespace"), r.PathValue("name")
	rows, err := p.readDeploymentSnapshot(r.Context())
	if err != nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "deployment snapshot could not be read"})
		return
	}
	for _, row := range rows {
		switch value := row.(type) {
		case deploymentView:
			if value.Namespace == namespace && value.Name == name {
				writeJSON(w, http.StatusOK, value)
				return
			}
		case deploymentSnapshotError:
			if value.Namespace == namespace && value.Name == name {
				writeJSON(w, http.StatusConflict, map[string]string{"error": value.Error})
				return
			}
		}
	}
	writeJSON(w, http.StatusNotFound, map[string]string{"error": "deployment could not be resolved"})
}

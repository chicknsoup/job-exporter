package main

import (
	"encoding/json"
	"errors"
	"github.com/gojektech/heimdall/httpclient"
	"github.com/prometheus/client_golang/prometheus"
	"io/ioutil"
	"log"
	"strings"
	"sync"
	"time"
)

type SparkJson struct {
	Apps struct {
		//App struct {
		SparkApps []SparkApp `json:"app"`
	} `json:"apps"`
}

type SparkApp struct {
	Id                     string  `json:"id"`
	User                   string  `json:"user"`
	Name                   string  `json:"name"`
	Queue                  string  `json:"queue"`
	FinalStatus            string  `json:"finalStatus"`
	StartTime              int64   `json:"startedTime"`
	ElapsedTime            int64   `json:"elapsedTime"`
	AMHostAddress          string  `json:"amHostHttpAddress"`
	AllocatedRAM           int64   `json:"allocatedMB"`
	AllocatedVCores        int64   `json:"allocatedVCores"`
	RunningContainers      int64   `json:"runningContainers"`
	QueueUsagePercentage   float64 `json:"queueUsagePercentage"`
	ClusterUsagePercentage float64 `json:"clusterUsagePercentage"`
}

type StormJson struct {
	StormApps []StormApp `json:"topologies"`
}

type StormApp struct {
	AssignedTotalMem float64 `json:"assignedTotalMem"`
	Owner            string  `json:"owner"`
	TopoName         string  `json:"name"`
	Status           string  `json:"status"`
	Uptime           int64   `json:"uptimeSeconds"`
}
type collector struct {
	sync.RWMutex
	yarnServers           string
	stormServers          string
	up                    *prometheus.Desc
	applicationsSubmitted *prometheus.Desc
	applicationsCompleted *prometheus.Desc
	applicationsPending   *prometheus.Desc
	applicationsRunning   *prometheus.Desc
	applicationsFailed    *prometheus.Desc
	applicationsKilled    *prometheus.Desc
	memoryReserved        *prometheus.Desc
	memoryAvailable       *prometheus.Desc
	memoryAllocated       *prometheus.Desc
	memoryTotal           *prometheus.Desc
	virtualCoresReserved  *prometheus.Desc
	virtualCoresAvailable *prometheus.Desc
	virtualCoresAllocated *prometheus.Desc
	virtualCoresTotal     *prometheus.Desc
	containersAllocated   *prometheus.Desc
	containersReserved    *prometheus.Desc
	containersPending     *prometheus.Desc
	nodesTotal            *prometheus.Desc
	nodesLost             *prometheus.Desc
	nodesUnhealthy        *prometheus.Desc
	nodesDecommissioned   *prometheus.Desc
	nodesDecommissioning  *prometheus.Desc
	nodesRebooted         *prometheus.Desc
	nodesActive           *prometheus.Desc
	scrapeFailures        *prometheus.Desc
	failureCount          int
	appMetrics            map[string]*prometheus.GaugeVec
}

const (
	yarnNamespace  = "yarn"
	metricAPI      = "/ws/v1/cluster/metrics"
	runningAppAPI  = "/ws/v1/cluster/apps?state=RUNNING"
	stormNamespace = "storm"
	stormAPI       = "/api/v1/topology/summary"
)

func newFuncMetric(metricName string, docString string, labels []string) *prometheus.Desc {
	return prometheus.NewDesc(prometheus.BuildFQName(yarnNamespace, "", metricName), docString, labels, nil)
}

func YarnCollector(servers string, storms string) *collector {
	appmetrics := map[string]*prometheus.GaugeVec{}
	return &collector{
		yarnServers:           servers,
		stormServers:          storms,
		up:                    newFuncMetric("up", "Able to contact YARN", []string{"yarn_instance"}),
		applicationsSubmitted: newFuncMetric("applications_submitted", "Total applications submitted", []string{"yarn_instance"}),
		applicationsCompleted: newFuncMetric("applications_completed", "Total applications completed", []string{"yarn_instance"}),
		applicationsPending:   newFuncMetric("applications_pending", "Applications pending", []string{"yarn_instance"}),
		applicationsRunning:   newFuncMetric("applications_running", "Applications running", []string{"yarn_instance"}),
		applicationsFailed:    newFuncMetric("applications_failed", "Total application failed", []string{"yarn_instance"}),
		applicationsKilled:    newFuncMetric("applications_killed", "Total application killed", []string{"yarn_instance"}),
		memoryReserved:        newFuncMetric("memory_reserved", "Memory reserved", []string{"yarn_instance"}),
		memoryAvailable:       newFuncMetric("memory_available", "Memory available", []string{"yarn_instance"}),
		memoryAllocated:       newFuncMetric("memory_allocated", "Memory allocated", []string{"yarn_instance"}),
		memoryTotal:           newFuncMetric("memory_total", "Total memory", []string{"yarn_instance"}),
		virtualCoresReserved:  newFuncMetric("virtual_cores_reserved", "Virtual cores reserved", []string{"yarn_instance"}),
		virtualCoresAvailable: newFuncMetric("virtual_cores_available", "Virtual cores available", []string{"yarn_instance"}),
		virtualCoresAllocated: newFuncMetric("virtual_cores_allocated", "Virtual cores allocated", []string{"yarn_instance"}),
		virtualCoresTotal:     newFuncMetric("virtual_cores_total", "Total virtual cores", []string{"yarn_instance"}),
		containersAllocated:   newFuncMetric("containers_allocated", "Containers allocated", []string{"yarn_instance"}),
		containersReserved:    newFuncMetric("containers_reserved", "Containers reserved", []string{"yarn_instance"}),
		containersPending:     newFuncMetric("containers_pending", "Containers pending", []string{"yarn_instance"}),
		nodesTotal:            newFuncMetric("nodes_total", "Nodes total", []string{"yarn_instance"}),
		nodesLost:             newFuncMetric("nodes_lost", "Nodes lost", []string{"yarn_instance"}),
		nodesUnhealthy:        newFuncMetric("nodes_unhealthy", "Nodes unhealthy", []string{"yarn_instance"}),
		nodesDecommissioned:   newFuncMetric("nodes_decommissioned", "Nodes decommissioned", []string{"yarn_instance"}),
		nodesDecommissioning:  newFuncMetric("nodes_decommissioning", "Nodes decommissioning", []string{"yarn_instance"}),
		nodesRebooted:         newFuncMetric("nodes_rebooted", "Nodes rebooted", []string{"yarn_instance"}),
		nodesActive:           newFuncMetric("nodes_active", "Nodes active", []string{"yarn_instance"}),
		scrapeFailures:        newFuncMetric("scrape_failures_total", "Number of errors while scraping YARN metrics", []string{"yarn_instance"}),
		appMetrics:            appmetrics,
	}
}

func (c *collector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.up
	ch <- c.applicationsSubmitted
	ch <- c.applicationsCompleted
	ch <- c.applicationsPending
	ch <- c.applicationsRunning
	ch <- c.applicationsFailed
	ch <- c.applicationsKilled
	ch <- c.memoryReserved
	ch <- c.memoryAvailable
	ch <- c.memoryAllocated
	ch <- c.memoryTotal
	ch <- c.virtualCoresReserved
	ch <- c.virtualCoresAvailable
	ch <- c.virtualCoresAllocated
	ch <- c.virtualCoresTotal
	ch <- c.containersAllocated
	ch <- c.containersReserved
	ch <- c.containersPending
	ch <- c.nodesTotal
	ch <- c.nodesLost
	ch <- c.nodesUnhealthy
	ch <- c.nodesDecommissioned
	ch <- c.nodesDecommissioning
	ch <- c.nodesRebooted
	ch <- c.nodesActive
	ch <- c.scrapeFailures
}

func (c *collector) Collect(ch chan<- prometheus.Metric) {
	c.resetMetrics()

	c.scrape()

	if c.yarnServers != "" {
		servers := strings.Split(c.yarnServers, ",")
		for _, server := range servers {
			up := 1.0
			//url:=
			data, err := fetch(server + metricAPI)
			if err != nil {
				up = 0.0
				c.failureCount++

				log.Println("Error while collecting data from YARN: " + err.Error())
			}

			if up == 0.0 {
				ch <- prometheus.MustNewConstMetric(c.scrapeFailures, prometheus.CounterValue, float64(c.failureCount), server)
				continue
			}

			metrics := data["clusterMetrics"]

			ch <- prometheus.MustNewConstMetric(c.applicationsSubmitted, prometheus.CounterValue, metrics["appsSubmitted"], server)
			ch <- prometheus.MustNewConstMetric(c.applicationsCompleted, prometheus.CounterValue, metrics["appsCompleted"], server)
			ch <- prometheus.MustNewConstMetric(c.applicationsPending, prometheus.GaugeValue, metrics["appsPending"], server)
			ch <- prometheus.MustNewConstMetric(c.applicationsRunning, prometheus.GaugeValue, metrics["appsRunning"], server)
			ch <- prometheus.MustNewConstMetric(c.applicationsFailed, prometheus.CounterValue, metrics["appsFailed"], server)
			ch <- prometheus.MustNewConstMetric(c.applicationsKilled, prometheus.CounterValue, metrics["appsKilled"], server)
			ch <- prometheus.MustNewConstMetric(c.memoryReserved, prometheus.GaugeValue, metrics["reservedMB"], server)
			ch <- prometheus.MustNewConstMetric(c.memoryAvailable, prometheus.GaugeValue, metrics["availableMB"], server)
			ch <- prometheus.MustNewConstMetric(c.memoryAllocated, prometheus.GaugeValue, metrics["allocatedMB"], server)
			ch <- prometheus.MustNewConstMetric(c.memoryTotal, prometheus.GaugeValue, metrics["totalMB"], server)
			ch <- prometheus.MustNewConstMetric(c.virtualCoresReserved, prometheus.GaugeValue, metrics["reservedVirtualCores"], server)
			ch <- prometheus.MustNewConstMetric(c.virtualCoresAvailable, prometheus.GaugeValue, metrics["availableVirtualCores"], server)
			ch <- prometheus.MustNewConstMetric(c.virtualCoresAllocated, prometheus.GaugeValue, metrics["allocatedVirtualCores"], server)
			ch <- prometheus.MustNewConstMetric(c.virtualCoresTotal, prometheus.GaugeValue, metrics["totalVirtualCores"], server)
			ch <- prometheus.MustNewConstMetric(c.containersAllocated, prometheus.GaugeValue, metrics["containersAllocated"], server)
			ch <- prometheus.MustNewConstMetric(c.containersReserved, prometheus.GaugeValue, metrics["containersReserved"], server)
			ch <- prometheus.MustNewConstMetric(c.containersPending, prometheus.GaugeValue, metrics["containersPending"], server)
			ch <- prometheus.MustNewConstMetric(c.nodesTotal, prometheus.GaugeValue, metrics["totalNodes"], server)
			ch <- prometheus.MustNewConstMetric(c.nodesLost, prometheus.GaugeValue, metrics["lostNodes"], server)
			ch <- prometheus.MustNewConstMetric(c.nodesUnhealthy, prometheus.GaugeValue, metrics["unhealthyNodes"], server)
			ch <- prometheus.MustNewConstMetric(c.nodesDecommissioned, prometheus.GaugeValue, metrics["decommissionedNodes"], server)
			ch <- prometheus.MustNewConstMetric(c.nodesDecommissioning, prometheus.GaugeValue, metrics["decommissioningNodes"], server)
			ch <- prometheus.MustNewConstMetric(c.nodesRebooted, prometheus.GaugeValue, metrics["rebootedNodes"], server)
			ch <- prometheus.MustNewConstMetric(c.nodesActive, prometheus.GaugeValue, metrics["activeNodes"], server)
		}
	}

	c.collectMetrics(ch)
	return
}

func (c *collector) scrape() {

	if c.yarnServers != "" {
		servers := strings.Split(c.yarnServers, ",")
		for _, server := range servers {
			uri := server + runningAppAPI
			data, er := fetchJson(uri)
			c.Lock()
			c.appMetrics[server] = prometheus.NewGaugeVec(
				prometheus.GaugeOpts{
					Namespace: yarnNamespace,
					Name:      "server",
					Help:      "Yarn server status",
				},
				[]string{
					"yarn_instance",
					"metric",
				},
			)
			c.Unlock()
			if er != nil {
				c.Lock()
				c.appMetrics[server].With(prometheus.Labels{"yarn_instance": server, "metric": "up"}).Set(0)
				c.Unlock()
				continue
			} else {
				c.Lock()
				c.appMetrics[server].With(prometheus.Labels{"yarn_instance": server, "metric": "up"}).Set(1)
				c.Unlock()
			}

			if data != nil {
				apps, err := fetchSparkJobs(data)

				if err != nil {
					log.Println("Couldn't fetch spark apps: %v", err)
					continue
				}

				for _, app := range apps {
					c.Lock()
					c.appMetrics[app.Id] = prometheus.NewGaugeVec(
						prometheus.GaugeOpts{
							Namespace: yarnNamespace,
							Name:      "running_spark_apps",
							Help:      "Currently running spark apps",
						},

						[]string{
							"id",
							"user",
							"name",
							"queue",
							"amhosthttpaddress",
							"yarn_instance",
							"metric",
						},
					)

					c.appMetrics[app.Id].With(prometheus.Labels{"id": app.Id, "user": app.User, "name": app.Name, "queue": app.Queue, "amhosthttpaddress": app.AMHostAddress, "yarn_instance": server, "metric": "up"}).Set(1)
					c.appMetrics[app.Id].With(prometheus.Labels{"id": app.Id, "user": app.User, "name": app.Name, "queue": app.Queue, "amhosthttpaddress": app.AMHostAddress, "yarn_instance": server, "metric": "elapsed_time"}).Set(float64(app.ElapsedTime))
					c.appMetrics[app.Id].With(prometheus.Labels{"id": app.Id, "user": app.User, "name": app.Name, "queue": app.Queue, "amhosthttpaddress": app.AMHostAddress, "yarn_instance": server, "metric": "final_status"}).Set(ConvertStatus(app.FinalStatus))
					c.appMetrics[app.Id].With(prometheus.Labels{"id": app.Id, "user": app.User, "name": app.Name, "queue": app.Queue, "amhosthttpaddress": app.AMHostAddress, "yarn_instance": server, "metric": "allocated_ram_mb"}).Set(float64(app.AllocatedRAM))
					c.appMetrics[app.Id].With(prometheus.Labels{"id": app.Id, "user": app.User, "name": app.Name, "queue": app.Queue, "amhosthttpaddress": app.AMHostAddress, "yarn_instance": server, "metric": "allocated_vcores"}).Set(float64(app.AllocatedVCores))
					c.appMetrics[app.Id].With(prometheus.Labels{"id": app.Id, "user": app.User, "name": app.Name, "queue": app.Queue, "amhosthttpaddress": app.AMHostAddress, "yarn_instance": server, "metric": "running_containers"}).Set(float64(app.RunningContainers))
					c.appMetrics[app.Id].With(prometheus.Labels{"id": app.Id, "user": app.User, "name": app.Name, "queue": app.Queue, "amhosthttpaddress": app.AMHostAddress, "yarn_instance": server, "metric": "queue_usage_percentage"}).Set(float64(app.QueueUsagePercentage))
					c.appMetrics[app.Id].With(prometheus.Labels{"id": app.Id, "user": app.User, "name": app.Name, "queue": app.Queue, "amhosthttpaddress": app.AMHostAddress, "yarn_instance": server, "metric": "cluster_usage_percentage"}).Set(float64(app.ClusterUsagePercentage))

					c.Unlock()

				}
			}

		}
	}

	if c.stormServers != "" {
		servers := strings.Split(c.stormServers, ",")
		for _, server := range servers {
			uri := (server + stormAPI)
			data, e := fetchJson(uri)
			c.Lock()
			c.appMetrics[server] = prometheus.NewGaugeVec(
				prometheus.GaugeOpts{
					Namespace: stormNamespace,
					Name:      "server",
					Help:      "Storm server status",
				},
				[]string{
					"storm_instance",
					"metric",
				},
			)
			c.Unlock()

			if e != nil {
				c.Lock()
				c.appMetrics[server].With(prometheus.Labels{"storm_instance": server, "metric": "up"}).Set(0)
				c.Unlock()
				continue
			} else {
				c.Lock()
				c.appMetrics[server].With(prometheus.Labels{"storm_instance": server, "metric": "up"}).Set(1)
				c.Unlock()
			}
			apps, err := fetchStormJobs(data)

			if err != nil {
				log.Println("Couldn't fetch storm apps: %v", err)
				continue
			}

			for _, app := range apps {
				c.Lock()
				c.appMetrics[app.TopoName] = prometheus.NewGaugeVec(
					prometheus.GaugeOpts{
						Namespace: stormNamespace,
						Name:      "running_storm_apps",
						Help:      "Currently running storm apps",
					},
					[]string{
						"name",
						"owner",
						"status",
						"metric",
					},
				)

				c.appMetrics[app.TopoName].With(prometheus.Labels{"name": app.TopoName, "owner": app.Owner, "status": app.Status, "metric": "up"}).Set(1)
				c.appMetrics[app.TopoName].With(prometheus.Labels{"name": app.TopoName, "owner": app.Owner, "status": app.Status, "metric": "assigned_total_mem"}).Set(float64(app.AssignedTotalMem))
				c.appMetrics[app.TopoName].With(prometheus.Labels{"name": app.TopoName, "owner": app.Owner, "status": app.Status, "metric": "uptime_seconds"}).Set(float64(app.Uptime))

				c.Unlock()

			}

		}
	}

}

func fetchSparkJobs(body []byte) ([]SparkApp, error) {
	var p SparkJson
	err := json.Unmarshal(body, &p)
	if err != nil {
		log.Println(err)
		return []SparkApp{}, nil
	}

	return p.Apps.SparkApps, nil
}

func fetchStormJobs(body []byte) ([]StormApp, error) {
	var p StormJson
	err := json.Unmarshal(body, &p)
	if err != nil {
		log.Println(err)
		return []StormApp{}, nil
	}

	return p.StormApps, nil
}

func fetchJson(url string) ([]byte, error) {
	timeout := 5000 * time.Millisecond
	client := httpclient.NewClient(httpclient.WithHTTPTimeout(timeout))
	resp, e := client.Get(url, nil)

	if e != nil {
		log.Println(e.Error())
		return []byte(`{"apps": {"app": []}}`), e
	}

	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return nil, errors.New("unexpected HTTP status: " + string(resp.StatusCode))
	}

	body, err := ioutil.ReadAll(resp.Body)

	if err != nil {
		return []byte(`{"apps": {"app": []}}`), err
	}

	return body, nil
}

func fetch(url string) (map[string]map[string]float64, error) {

	timeout := 5000 * time.Millisecond
	client := httpclient.NewClient(httpclient.WithHTTPTimeout(timeout))
	resp, err := client.Get(url, nil)

	if err != nil {
		return nil, err
	}

	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return nil, errors.New("unexpected HTTP status: " + string(resp.StatusCode))
	}

	body, err := ioutil.ReadAll(resp.Body)

	if err != nil {
		return nil, err
	}

	var data map[string]map[string]float64

	err = json.Unmarshal(body, &data)

	if err != nil {
		return nil, err
	}

	return data, nil
}

func (c *collector) resetMetrics() {
	c.RLock()
	defer c.RUnlock()
	for _, m := range c.appMetrics {
		m.Reset()
	}
}

func (c *collector) collectMetrics(metrics chan<- prometheus.Metric) {
	c.RLock()
	defer c.RUnlock()
	for _, m := range c.appMetrics {
		m.Collect(metrics)
	}
}

func ConvertStatus(runningStatus string) float64 {
	switch runningStatus {
	case "UNDEFINED":
		return -1
	case "SUCCEEDED":
		return 0
	case "FAILED":
		return 1
	case "KILLED":
		return 2
	default:
		return -1
	}
}

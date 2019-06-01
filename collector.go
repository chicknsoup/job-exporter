package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/PuerkitoBio/goquery"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/shirou/gopsutil/process"
	"io/ioutil"
	"job-exporter/overseer"
	"log"
	"net/http"
	"os"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"sync"
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

type StreamingStats struct {
	App             *SparkApp
	CompletedBaches int64
	LastBatchID     string
	BatchInput      int64
	BatchDuration   int64
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
	client                *http.Client
}

const (
	yarnNamespace          = "yarn"
	metricAPI              = "/ws/v1/cluster/metrics"
	runningAppAPI          = "/ws/v1/cluster/apps?state=RUNNING"
	stormNamespace         = "storm"
	stormAPI               = "/api/v1/topology/summary"
	streamingStatsEndpoint = "/streaming/statistics" //proxy/app_id/streaming/statistics
)

var (
	is_restarting bool
)

func SetRestartState(state bool) {
	is_restarting = state
}

func newFuncMetric(metricName string, docString string, labels []string) *prometheus.Desc {
	return prometheus.NewDesc(prometheus.BuildFQName(yarnNamespace, "", metricName), docString, labels, nil)
}

func YarnCollector(client *http.Client, servers string, storms string) *collector {
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
		client:                client,
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
			data, err := c.fetch(server + metricAPI)
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

func (c *collector) getAppMetrics(server string, app SparkApp, ch chan<- *StreamingStats, wg *sync.WaitGroup) {
	defer wg.Done()
	stats, e := c.fetchSparkJobStat(server, app)

	if e != nil {
		log.Println("Failed to fetch streaming stats for appid=" + app.Id)
		stats = nil
	}
	ch <- stats
}

func (c *collector) scrape() {

	pid := os.Getpid()
	//fmt.Println(pid)
	proc, err := process.NewProcess(int32(pid))
	if err == nil {
		meminfo, _ := proc.MemoryInfoEx()
		//auto restart
		if meminfo != nil {
			if (meminfo.RSS > memlimit) {
				if !is_restarting {
					fmt.Println(fmt.Sprintf("job exporter is restarting due to mem limit reached, current value = (  %.6f MB)", meminfo.RSS/1024/1024))
					overseer.Restart()
					SetRestartState(true)
				}
			}
		}
	}

	if c.yarnServers != "" {

		servers := strings.Split(c.yarnServers, ",")
		for _, server := range servers {
			uri := server + runningAppAPI
			data, er := c.fetchJson(uri)
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

				ch := make(chan *StreamingStats)
				var wg sync.WaitGroup

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

					wg.Add(1)

					go c.getAppMetrics(server, app, ch, &wg)
				}
				// close the channel in the background
				go func() {
					wg.Wait()
					close(ch)
				}()

				for res := range ch {
					//log.Println(res.App.Id)
					if res != nil {
						c.Lock()
						c.appMetrics[res.App.Id].With(prometheus.Labels{"id": res.App.Id, "user": res.App.User, "name": res.App.Name, "queue": res.App.Queue, "amhosthttpaddress": res.App.AMHostAddress, "yarn_instance": server, "metric": "last_streaming_batch_input"}).Set(float64(res.BatchInput))
						c.appMetrics[res.App.Id].With(prometheus.Labels{"id": res.App.Id, "user": res.App.User, "name": res.App.Name, "queue": res.App.Queue, "amhosthttpaddress": res.App.AMHostAddress, "yarn_instance": server, "metric": "last_streaming_batch_duration_seconds"}).Set(float64(res.BatchDuration))
						c.appMetrics[res.App.Id].With(prometheus.Labels{"id": res.App.Id, "user": res.App.User, "name": res.App.Name, "queue": res.App.Queue, "amhosthttpaddress": res.App.AMHostAddress, "yarn_instance": server, "metric": "total_completed_batches"}).Set(float64(res.CompletedBaches))

						c.Unlock()
					}
				}
			}

		}
	}

	if c.stormServers != "" {
		servers := strings.Split(c.stormServers, ",")
		for _, server := range servers {
			uri := (server + stormAPI)
			data, e := c.fetchJson(uri)
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
						"storm_instance",
						"metric",
					},
				)

				c.appMetrics[app.TopoName].With(prometheus.Labels{"name": app.TopoName, "owner": app.Owner, "status": app.Status, "storm_instance": server, "metric": "up"}).Set(1)
				c.appMetrics[app.TopoName].With(prometheus.Labels{"name": app.TopoName, "owner": app.Owner, "status": app.Status, "storm_instance": server, "metric": "assigned_total_mem"}).Set(float64(app.AssignedTotalMem))
				c.appMetrics[app.TopoName].With(prometheus.Labels{"name": app.TopoName, "owner": app.Owner, "status": app.Status, "storm_instance": server, "metric": "uptime_seconds"}).Set(float64(app.Uptime))

				c.Unlock()

			}

		}
	}

}

func
fetchSparkJobs(body []byte) ([]SparkApp, error) {
	var p SparkJson
	err := json.Unmarshal(body, &p)
	if err != nil {
		log.Println(err)
		return []SparkApp{}, nil
	}

	return p.Apps.SparkApps, nil
}

func
fetchStormJobs(body []byte) ([]StormApp, error) {
	var p StormJson
	err := json.Unmarshal(body, &p)
	if err != nil {
		log.Println(err)
		return []StormApp{}, nil
	}

	return p.StormApps, nil
}

func (c *collector) fetchJson(url string) ([]byte, error) {
	//timeout := 5000 * time.Millisecond
	//client := httpclient.NewClient(httpclient.WithHTTPTimeout(timeout))
	resp, e := c.client.Get(url)

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

func (c *collector) fetch(url string) (map[string]map[string]float64, error) {

	//timeout := 5000 * time.Millisecond
	//client := httpclient.NewClient(httpclient.WithHTTPTimeout(timeout))
	resp, err := c.client.Get(url)

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

func (c *collector) fetchSparkJobStat(server string, app SparkApp) (*StreamingStats, error) {

	var mem runtime.MemStats
	runtime.ReadMemStats(&mem)

	url := server + "/proxy/" + app.Id + streamingStatsEndpoint

	//var tbatch_pattern =`Completed Batches.*?out of (\d+)`
	//timeout := 30000 * time.Millisecond

	//client := httpclient.NewClient(httpclient.WithHTTPTimeout(timeout))
	resp, err := c.client.Get(url)

	if err != nil {
		return nil, err
	}

	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return nil, errors.New("unexpected HTTP status: " + string(resp.StatusCode))
	}

	body, err := ioutil.ReadAll(resp.Body)

	if isdebug {
		log.Printf("[1]Allocated memory: %fMB. Number of goroutines: %d", float32(mem.Alloc)/1024.0/1024.0, runtime.NumGoroutine())
	}

	if err != nil {
		return nil, err
	}

	var data StreamingStats

	doc, err := goquery.NewDocumentFromReader(strings.NewReader(string(body)))

	if err != nil {
		return nil, err
	}

	complete_batches := strings.TrimSpace(doc.Find("h4#completed").Text())
	r := regexp.MustCompile("out of (\\d+)")
	tmp := r.FindAllString(complete_batches, -1)

	if len(tmp) > 0 {
		data.CompletedBaches, _ = strconv.ParseInt(strings.TrimSpace(strings.Replace(tmp[0], "out of ", "", -1)), 10, 64)
	}

	// Find completed-batches table
	doc.Find("table#completed-batches-table").Each(func(index int, tablehtml *goquery.Selection) {
		rowhtml := tablehtml.Find("tr:first-of-type")
		rowhtml.Find("td").Each(func(indexth int, tablecell *goquery.Selection) {

			if batch_id, ok := tablecell.Attr("id"); ok {
				data.LastBatchID = batch_id
			}

			val := strings.Split(strings.TrimSpace(tablecell.Text()), " ")[0]
			if indexth == 1 {
				data.BatchInput, _ = strconv.ParseInt(val, 10, 64)
			}
			if indexth == 3 {
				data.BatchDuration, _ = strconv.ParseInt(val, 10, 64)
			}

			//fmt.Println(strings.Split(strings.TrimSpace(tablecell.Text())," ")[0])
		})

	})

	if isdebug {
		log.Printf("[2]Allocated memory: %fMB. Number of goroutines: %d", float32(mem.Alloc)/1024.0/1024.0, runtime.NumGoroutine())
	}

	data.App = &app
	return &data, nil
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

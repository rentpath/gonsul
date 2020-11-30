package main

import (
	"encoding/base64"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"path/filepath"
	"strings"

	"gopkg.in/yaml.v3"
)

var (
	consulDC string
	consulUrl string
	consulPrefix string
	rootPath string

	inserts   = 0
	updates   = 0
	deletes   = 0
	unchanged = 0

	bodyStruct []ConsulResult
	operations []string
	transactions []string
	transactionsSets [][]string

	localData = make(map[string]string)
	liveData = make(map[string]string)
)

type ConsulResult struct {
	LockIndex   int    `json:"lockIndex"`
	Key         string `json:"Key"`
	Flags       int    `json:"Flags"`
	Value       string `json:"Value"`
	CreateIndex int    `json:"CreateIndex"`
	ModifyIndex int    `json:"ModifyIndex"`
}

func main() {
	defer func() {
		if r := recover(); r != nil {
			fmt.Println("[FATAL] " + r.(string))
			os.Exit(1)
		}
	}()
	flag.StringVar(&consulUrl, "consul-url", "http://127.0.0.1:8500", "(REQUIRED) The Consul URL REST API endpoint (Full URL with scheme)")
	flag.StringVar(&consulPrefix, "consul-prefix", "", "The base KV path will be prefixed to dir path")
	flag.StringVar(&consulDC, "consul-datacenter", "", "The Consul datacenter to use")
	flag.StringVar(&rootPath, "root-path", ".", "The base directory to walk for files")
	flag.Parse()
	filepath.Walk(rootPath, func(walkPath string, info os.FileInfo, err error) error {
		if info.IsDir() && strings.HasPrefix(info.Name(), ".") && info.Name() != "." {
			return filepath.SkipDir
		} else if filepath.Ext(walkPath) == ".yml" {
			content, err := ioutil.ReadFile(walkPath)
			if err != nil {
				panic(fmt.Sprintf("error reading YAML file: %s with Message: %s", walkPath, err.Error()))
			}
			var parsedYAML map[string]string
			err = yaml.Unmarshal([]byte(content), &parsedYAML)
			if err != nil {
				panic(fmt.Sprintf("error parsing YAML file: %s with Message: %s", walkPath, err.Error()))
			}
			for key, value := range parsedYAML {
				localData[filepath.Join(consulPrefix, strings.TrimSuffix(walkPath, filepath.Ext(walkPath)), key)] = value
			}
		}
		return nil
	})
	resp, err := http.Get(fmt.Sprintf("%s/v1/kv/%s/?recurse=true", consulUrl, consulPrefix))
	if err != nil {
		panic("DoGET: "+err.Error())
	}
	if resp.StatusCode >= 400 && resp.StatusCode != 404 {
		panic("Invalid response from consul: "+resp.Status)
	}
	if resp.StatusCode != 404 {
		bodyBytes, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			panic("ReadGetResponse: "+err.Error())
		}
		bodyString := string(bodyBytes)
		err = json.Unmarshal([]byte(bodyString), &bodyStruct)
		if err != nil {
			panic("Unmarshal: "+err.Error())
		}
		for _, v := range bodyStruct {
			liveData[v.Key] = v.Value
		}
	}
	for localKey, localVal := range localData {
		localValB64 := base64.StdEncoding.EncodeToString([]byte(localVal))
		setPayload := fmt.Sprintf("{\"KV\":{\"Verb\":\"set\",\"Key\":\"%s\",\"Value\":\"%s\"}}", localKey, localValB64)
		if liveVal, ok := liveData[localKey]; ok {
			if localValB64 != liveVal {
				updates++
				operations = append(operations, setPayload)
			} else {
				unchanged++
			}
		} else {
			inserts++
			operations = append(operations, setPayload)
		}
	}
	for liveKey := range liveData {
		if _, ok := localData[liveKey]; !ok {
			deletes++
			operations = append(operations, fmt.Sprintf("{\"KV\":{\"Verb\":\"delete\",\"Key\":\"%s\"}}", liveKey))
		}
	}
	for _, op := range operations {
		if len(transactions) == 64 || len(strings.Join(transactions, ",") + op) > 500000 {
			transactionsSets = append(transactionsSets, transactions)
			transactions = []string{}
		}
		transactions = append(transactions, op)
	}
	transactionsSets = append(transactionsSets, transactions)
	for _, transactions := range transactionsSets {
		jsonPayload := strings.NewReader("[" + strings.Join(transactions, ",") + "]")
		txnUrl := consulUrl + "/v1/txn"
		if consulDC != "" {
			txnUrl = txnUrl + "?dc=" + consulDC
		}
		req, err := http.NewRequest("PUT", txnUrl, jsonPayload)
		if err != nil {
			panic("NewRequestPUT: "+err.Error())
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			panic("PUT: "+err.Error())
		}
		if resp.StatusCode != 200 {
			bodyBytes, err := ioutil.ReadAll(resp.Body)
			if err != nil {
				panic("ReadResponse: "+err.Error())
			}
			panic("TransactionError: "+string(bodyBytes))
		}
		for _, txn := range transactions {
			fmt.Println("[INFO] " + txn)
		}
	}
	fmt.Println("[INFO] " + fmt.Sprintf("Finished: %d Inserts, %d Updates, %d Deletes, %d Unchanged", inserts, updates, deletes, unchanged))
}

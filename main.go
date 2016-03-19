package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"log"
	"net/http"
	"net/url"
	"os"
	"strings"
)

type SlackPayload struct {
	Text      string `json:"text"`
	Username  string `json:"username"`
	IconEmoji string `json:"icon_emoji"`
}

const SlackAPIUrl string = "##TOKEN##"

var slackNotification bool

func main() {

	dryRun := flag.Bool("dryRun", false, "If true, data won't be deleted")

	flag.Parse()
	log.SetOutput(os.Stdout)

	slackNotification = !*dryRun

	if len(flag.Args()) < 1 {
		log.Println("Usage: s3-garbage-collector filepath(e.g /home/panda/s3.log)")
		os.Exit(1)
	}

	filepath := flag.Args()[0]
	log.Printf("filepath : %s \n", filepath)

	inFile, _ := os.Open(filepath)
	defer inFile.Close()
	scanner := bufio.NewScanner(inFile)
	scanner.Split(bufio.ScanLines)

	svc := s3.New(&aws.Config{Region: aws.String("us-west-2")})

	for scanner.Scan() {
		row := scanner.Text()
		data := strings.Split(row, ",")
		if len(data) == 2 && data[0] != "SKIP" {
			params := &s3.DeleteObjectInput{
				Bucket: aws.String(data[0]),
				Key:    aws.String(data[1]),
			}
			svc.DeleteObject(params)
			_, err := svc.DeleteObject(params)
			isError(err, data[1])
		}
	}

	defer os.Remove(filepath)
}

func isError(err error, key string) {
	if err != nil {
		log.Println(key)
		if slackNotification {
			notifyToSlack(err.Error())
		}
	}
}

func notifyToSlack(msg string) {
	payload := SlackPayload{Text: msg, Username: "I'm S3 Garbage Collector", IconEmoji: ":construction_worker:"}
	j, _ := json.Marshal(payload)
	data := url.Values{}
	data.Set("payload", string(j))
	resp, _ := http.PostForm(SlackAPIUrl, data)
	defer resp.Body.Close()
}

#!/bin/bash

IMAGE=gcr.io/cedar-router-268801/us-central1/poc:latest
TEMPLATE=gs://cedar-router-beam-poc/template/poc-template.json

docker image build -t $IMAGE .
gcloud auth print-access-token | docker login -u oauth2accesstoken --password-stdin https://gcr.io
docker push $IMAGE

gcloud dataflow flex-template build $TEMPLATE --image $IMAGE --sdk-language JAVA
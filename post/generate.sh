#!/bin/bash

protoc post/postpb/post.proto --go_out=plugins=grpc:.
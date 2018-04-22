#!/usr/bin/env bash

spark-submit --class "App" \
             --packages org.apache.pdfbox:pdfbox:2.0.9,org.scalaj:scalaj-http_2.11:2.3.0 \
             --master local \
             target/scala-2.11/findidwikidatawithfewdata_2.11-0.1.jar pdf/exemple.pdf

# EMR HBase Health Check Tool

## Description

A simple tool which checks the health status of EMR HBase clusters based on region server jmx metrics.

## Features

- Support multiple EMR clusters
- Analyze hot regions and tables
- Write results to an Excel file

### Check Items
- Region counts in all region servers
- If any region file size larger than 10GB
- Region server request statistics to identify hot region servers
- Table request statistics to identity hot row keys
- HBase configuration check
- Region server store file count
- Cache hit ratio
- Region split times
- GC count and time

## Getting Started

### Installation
python >= 3.6

```
pip3 install .
```

## Usage
Collect EMR HBase clusters information. If cluster id is not set all EMR HBase clusters will be collected.
```
hbhc collect-info --clusters j-xxxxx,j-yyyyy
```
Generate report. 
```
hbhc report
```
You can find the generated report in the report directory.

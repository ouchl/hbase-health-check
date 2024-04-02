# EMR HBase Health Check Tool

## Description

A simple tool which checks the health status of EMR HBase clusters based on region server jmx metrics.

## Features

- Support multiple EMR clusters
- Analyze hot regions and tables
- Write results to an Excel file

## Getting Started

### Installation
python >= 3.6

```
pip3 install .
```

## Usage
Collect EMR HBase clusters information. If cluster id is not set all EMR HBase clusters will be collected.
```
hbhc collect-info --cluster-id j-xxxxx,j-yyyyy
```
Generate report. 
```
hbhc report
```

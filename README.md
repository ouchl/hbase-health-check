# EMR HBase Health Check Tool

## Description

This tool checks the health status of EMR HBase clusters based on region server jmx metrics.

## Features

- Support multiple EMR clusters
- Write results to an Excel file
- Analyze hot regions and tables

## Getting Started

### Installation

```
pip install .
```

## Usage
Collect EMR HBase clusters information. If cluster id is not set all EMR HBase clusters will be collected.
```
hbhc collect-info --cluster-id j-xxxxx
```
Generate report. 
```
hbhc report
```

# sar2influx

This is an incomplete converter from sysstat's SADF CSV output to InfluxDB for
easier querying.

## Usage

```shell
# Convert your SAR data using SADF
sadf -d input.dat -- -A > mydata.csv
# Convert the CSV to influx format
./sar2influx.py mydata.csv > mydata.metrics
# Push to influx
curl -i -XPOST 'http://localhost:8086/write?db=mydb' --data-binary @mydata.metrics
```

## License

MIT
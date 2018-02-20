#!/usr/bin/env python

from __future__ import print_function
import csv
import datetime
import sys
import time

def usage():
	print("usage: %s <file.csv>" % sys.argv[0], file=sys.stderr)
	sys.exit(1)

def output_influx(data, name, value, additional_tags):
	additional_tags["hostname"] = data["hostname"]
	tags = ",".join(["=".join(x) for x in additional_tags.items()])
	event_time = datetime.datetime.strptime(data["timestamp"], "%Y-%m-%d %H:%M:%S %Z")
	timestamp = "%d%s" % (
		time.mktime(event_time.timetuple()),
		"0" * 9
	)
	print("%s,%s value=%s %s" % (
		name,
		tags,
		value,
		timestamp
	))

def convert_cpu(data):
	cpu = data["CPU"]
	if cpu == "-1":
		cpu = "ALL"

	tags = { "cpu": cpu	}

	output_influx(data, "cpu_user", data[r"%usr"], tags)
	output_influx(data, "cpu_nice", data[r"%nice"], tags)
	output_influx(data, "cpu_sys", data[r"%sys"], tags)
	output_influx(data, "cpu_iowait", data[r"%iowait"], tags)
	output_influx(data, "cpu_steal", data[r"%steal"], tags)
	output_influx(data, "cpu_irq", data[r"%irq"], tags)
	output_influx(data, "cpu_soft", data[r"%soft"], tags)
	output_influx(data, "cpu_guest", data[r"%guest"], tags)
	output_influx(data, "cpu_idle", data[r"%idle"], tags)

# to try and prevent interrupts from blowing grafana, only output metrics
# that have at least one data point above 0
intr_cache = {}
def convert_intr(data):
	interrupt = data["INTR"]
	value = data["intr/s"]

	if interrupt == "-1":
		interrupt = "ALL"

	tags = { "interrupt": interrupt }

	if float(value) > 0.0:
		# output the previous 0.0 value
		if interrupt in intr_cache and float(intr_cache[interrupt]["intr/s"]) == 0.0:
			cache = intr_cache[interrupt]
			output_influx(cache, "interrupt", cache["intr/s"], tags)

		output_influx(data, "interrupt", value, tags)

	intr_cache[interrupt] = data

def convert_pages(data):
	tags = { }

	output_influx(data, "page_in", data["pgpgin/s"], tags)
	output_influx(data, "page_out", data["pgpgout/s"], tags)
	output_influx(data, "fault", data["fault/s"], tags)
	output_influx(data, "major_fault", data["majflt/s"], tags)
	output_influx(data, "page_free", data["pgfree/s"], tags)
	output_influx(data, "page_scan", data["pgscank/s"], tags)
	output_influx(data, "page_steal", data["pgsteal/s"], tags)
	output_influx(data, "page_vm_eff", data[r"%vmeff"], tags)

def convert_memory(data):
	tags = { }

	output_influx(data, "memory_free", data["kbmemfree"], tags)
	output_influx(data, "memory_used", data["kbmemused"], tags)
	output_influx(data, "memory_used_pc", data[r"%memused"], tags)
	output_influx(data, "memory_buffers", data["kbbuffers"], tags)
	output_influx(data, "memory_cached", data["kbcached"], tags)
	output_influx(data, "memory_commit", data["kbcommit"], tags)
	output_influx(data, "memory_commit_pc", data[r"%commit"], tags)

def convert_swap(data):
	tags = { }

	output_influx(data, "swap_free", data["kbswpfree"], tags)
	output_influx(data, "swap_used", data["kbswpused"], tags)
	output_influx(data, "swap_used_pc", data[r"%swpused"], tags)
	output_influx(data, "swap_cached", data["kbswpcad"], tags)
	output_influx(data, "swap_cached_pc", data[r"%swpcad"], tags)

def convert_load(data):
	tags = { }

	output_influx(data, "runq_size", data["runq-sz"], tags)
	output_influx(data, "process_list_size", data["plist-sz"], tags)
	output_influx(data, "load_avg1", data["ldavg-1"], tags)
	output_influx(data, "load_avg5", data["ldavg-5"], tags)
	output_influx(data, "load_avg15", data["ldavg-15"], tags)

def convert_disk(data):
	tags = { "disk": data["DEV"] }

	output_influx(data, "disk_tps", data["tps"], tags)
	output_influx(data, "disk_read", data["rd_sec/s"], tags)
	output_influx(data, "disk_write", data["wr_sec/s"], tags)
	output_influx(data, "disk_req_size_avg", data["avgrq-sz"], tags)
	output_influx(data, "disk_queue_size_avg", data["avgqu-sz"], tags)
	output_influx(data, "disk_await", data["await"], tags)
	output_influx(data, "disk_service_time", data["svctm"], tags)
	output_influx(data, "disk_usage", data[r"%util"], tags)

def convert(data):
	# try to look at the existing fields to find the proper conversion
	if "CPU" in data and r"%usr" in data:
		convert_cpu(data)
	elif "INTR" in data:
		convert_intr(data)
	elif "pgpgin/s" in data:
		convert_pages(data)
	elif "kbmemfree" in data:
		convert_memory(data)
	elif "kbswpfree" in data:
		convert_swap(data)
	elif "ldavg-15" in data:
		convert_load(data)
	elif "DEV" in data and "rd_sec/s" in data:
		convert_disk(data)

def read_file(filename):
	with open(filename, newline='') as csvfile:
		reader = csv.reader(csvfile, delimiter=';')
		headers = []
		for row in reader:
			if row[0][0] == '#':
				headers = [row[0][2:]] + row[1:]
				continue

			convert(dict(zip(headers, row)))

if __name__ == "__main__":
	if len(sys.argv) < 2:
		usage()

	try:
		read_file(sys.argv[1])
	except FileNotFoundError as e:
		print("error: file %s was not found" % e.filename, file=sys.stderr)
		usage()
	except Exception as e:
		print("unknown error: %s" % e)
		usage()
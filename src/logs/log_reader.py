import os
import pandas as pd
import re
import time
from datetime import datetime

def latest_log_file(suffix):
    log_files = [f for f in os.listdir('.') if f.endswith(f'_{suffix}.log')]
    latest_log_file = max(log_files, key=os.path.getmtime)
    log_time = os.path.getmtime(latest_log_file)
    log_date = datetime.fromtimestamp(log_time).strftime('%Y-%m-%d')
    with open(latest_log_file, 'r') as file:
        lines = file.readlines()
    return lines,log_date

def converter_etl(lines):
    items = []
    current_table = None
    read_time = treat_time = write_time = test_time = row_count = total_time = 0.0

    for line in lines:
        match = re.search(r'(GOLD|SILVER|BRONZE): (\S+)', line)
        if match:
            if current_table:
                item = {
                    'table': current_table,
                    'level': level,
                    'read_time': read_time,
                    'treat_time': treat_time,
                    'write_time': write_time,
                    'test_time': test_time,
                    'row_count': row_count,
                    'total_time': total_time
                }
                items.append(item)
            level = match.group(1)
            current_table = match.group(2)
            read_time = treat_time = write_time = test_time = row_count = total_time = 0.0
        if 'read_data' in line:
            match = re.search(r'read_data: ([\d.]+)s', line)
            if match:
                read_time += float(match.group(1))
        if 'treat_data' in line:
            match = re.search(r'treat_data: ([\d.]+)s', line)
            if match:
                treat_time = float(match.group(1))
        if 'write_data' in line:
            match = re.search(r'write_data: ([\d.]+)s', line)
            if match:
                write_time = float(match.group(1))
        if 'test_data' in line:
            match = re.search(r'test_data: ([\d.]+)s', line)
            if match:
                test_time = float(match.group(1))
        if 'rows' in line:
            match = re.search(r'rows: (\d+)', line)
            if match:
                row_count = int(match.group(1))
        if current_table and re.search(rf'{current_table}: ([\d.]+)s', line):
            match = re.search(rf'{current_table}: ([\d.]+)s', line)
            if match:
                total_time = float(match.group(1))


    if current_table:
        item = {
            'table': current_table,
            'level': level,
            'read_time': read_time,
            'treat_time': treat_time,
            'write_time': write_time,
            'test_time': test_time,
            'row_count': row_count,
            'total_time': total_time
        }
        items.append(item)

    return items

def export(items,output):
    data = pd.DataFrame(items)
    data['total_time'] = data.apply(lambda row: row['read_time'] + row['treat_time'] + row['write_time'] if row['total_time'] == 0.0 else row['total_time'], axis=1)
    data.to_csv(output, index=False)

def converter_ingestion() -> None:
    return

def converter(suffix):
    lines, log_date = latest_log_file(suffix)
    items = converter_etl(lines)
    export(items,f'monitoring/{log_date}_etl.csv')

suffix = 'ETL'
converter(suffix)
from optparse import OptionParser
import requests
import json
import re
import os
import shutil
from datetime import datetime, timedelta
import time
from bs4 import BeautifulSoup
from boto.s3.connection import S3Connection
from boto.s3.key import Key
from boto.utils import parse_ts

import config

# python yahnr.py --get --process --combine --upload
# python yahnr.py --process --combine --upload
# python yahnr.py --deploy

# Set up Regexes
RE_NUM = re.compile(r'\d+')
RE_TIME_AGO = re.compile(r'\d+\s\w+?\sago')

def getS3Bucket():
    conn = S3Connection(config.AWS_ACCESS_KEY_ID, config.AWS_SECRET_ACCESS_KEY)
    bucket = conn.create_bucket(config.AWS_STORAGE_BUCKET_NAME)
    bucket.set_acl('public-read')
    return bucket

# Download the first page of HN
def get(filename):
    f = open(filename,'w')

    url = 'http://news.ycombinator.com/news'
    r = requests.get(url)
    if r.status_code == 200:
        f.write(r.content)
        f.close()
    else:
        print "Could not get HN feed"
        exit()

# Extract info from the HTML
def process(infile, outfile):
    f = open(infile,'r')
    soup = BeautifulSoup(f.read())
    summary = []

    now = time.time()
    print 'Current epoch time: %d' % int(now)

    for row in soup.find_all('tr')[2:]:
        if len(row.find_all('td')) == 3:
            cells = row.find_all('td')
            info_row = row.next_sibling
            # print row.text, info_row.text

            # First row
            order = cells[0].text
            link_data = cells[2]
            title = cells[2].find('a').text
            url = cells[2].find('a')['href']
            domain = cells[2].find('span').text if cells[2].find('span') else ''

            # Second row
            points = info_row.find('span').text if info_row.find('span') else ''
            user = info_row.find('a').text if info_row.find('a') else ''
            num_comments = info_row.find_all('a')[-1].text if 'comment' in info_row.find_all('a')[-1].text else ''
            thread_id = info_row.find_all('a')[-1]['href'] if 'comment' in info_row.find_all('a')[-1].text else ''
            time_ago_str = RE_TIME_AGO.search(info_row.text).group(0)

            num, time_type, other = time_ago_str.split(' ')
            if 'minute' in time_type:
                posted = now - int(num) * 60
            elif 'hour' in time_type:
                posted = now - int(num) * 60 * 60
            elif 'day' in time_type:
                posted = now - int(num) * 60 * 60 * 24
            else:
                print 'Error processing time ago string: %s' % time_ago_str
                exit()

            thread_type = 'Jobs' if thread_id == '' else 'Other'

            data = {'order' : RE_NUM.search(order).group(0),
                    'title' : title,
                    'url'   : url,
                    'domain': domain.strip('() '),
                    'points': int(RE_NUM.search(points).group(0)) if 'point' in points else 0,
                    'user'  : user,
                    'num_comments' : int(RE_NUM.search(num_comments).group(0)) if 'comments' in num_comments else 0,
                    'thread_id' : RE_NUM.search(thread_id).group(0) if 'item' in thread_id else '',
                    'type' : thread_type,
                    'posted_time' : int(posted),
                    }
            summary.append(data)
    f.close()

    print json.dumps(summary, indent=2)

    o = open(outfile,'w')
    o.write(json.dumps(summary, indent=2))
    o.close()

# Combine the recent files into a big file of data
def combine(now):
    recent_24 = now - timedelta(hours=24)
    # Combine files
    current_data = []
    datetime_cnt = recent_24
    while datetime_cnt <= now:
        print 'Processing data for %s?' % datetime_cnt.strftime('%Y-%m-%d-%H-%M'),
        fn = 'hn-data-%s.json' % datetime_cnt.strftime('%Y-%m-%d-%H-%M')
        fp = os.path.join('data', fn)
        if os.path.exists(fp):
            print 'Y'
            f = open( fp, 'r')
            current_data.extend(json.loads(f.read()))
            f.close()
        else:
            print 'N'
        datetime_cnt += timedelta(minutes=15)

    print 'Got %d rows' % len(current_data)

    f = open(os.path.join('data', 'now.json'),'w')
    f.write(json.dumps(current_data,indent=2))
    f.close()

    # Save this snapshot if it's midnight so weh have a daily history
    if now.hour == 0 and now.minute == 0:
        fn = 'hn-data-%s.json' % now.strftime('%Y-%m-%d')
        shutil.copyfile(os.path.join('data', 'now.json'), os.path.join('data', fn))

# Upload the latest files
def upload(now):
    bucket = getS3Bucket()
    files = ['now.json', 'hn-data-%s.json' % now.strftime('%Y-%m-%d-%H-%M'), 'hn-data-%s.json' % now.strftime('%Y-%m-%d')]
    for fn in files:
        print 'Uploading %s?' % fn,
        fp = os.path.join('data',fn)
        if os.path.exists(fp):
            print 'Y'
            k = Key(bucket)
            k.key = 'data/' + fn
            if fn is not 'now.json':
                k.set_metadata('Cache-Control', 'max-age=604800')
                k.set_metadata('Expires', (datetime.now() + timedelta(days=7300)).strftime("%a, %d %b %Y %H:%M:%S GMT"))
            k.set_contents_from_filename(fp)
            k.set_acl('public-read')
        else:
            print 'N'

# Deploy the web server to the static S3 site
def deploy(dirs = ['.','js','css','img'], exts = ['html', 'js', 'css', 'png', 'json']):
    bucket = getS3Bucket()
    upload_dirs = dirs
    upload_ext  = set(exts)
    for d in upload_dirs:
        for fn in os.listdir(d):
            if not os.path.isdir(os.path.join(d,fn)) and fn.split('.')[-1] in upload_ext:
                print 'Uploading', fn
                k = Key(bucket)
                k.key = d + '/' + fn if d is not '.' else fn
                if fn is not 'now.json':
                    k.set_metadata('Cache-Control', 'max-age=21600')
                    k.set_metadata('Expires', (datetime.now() + timedelta(hours=6)).strftime("%a, %d %b %Y %H:%M:%S GMT"))
                k.set_contents_from_filename(os.path.join(d,fn))
                k.set_acl('public-read')

# Remove old data S3
def clean_s3(now):
    remove_datetime = now - timedelta(hours=24)
    bucket = getS3Bucket()
    for key in bucket.list():
        key_name = key.name.encode('utf-8')
        key_datetime = parse_ts(key.last_modified)
        if 'data' in key_name and len(key_name) == 34 and key_datetime < remove_datetime:
            print key_name, 'Remove'
            bucket.delete_key(key_name)
        else:
            print key_name, 'Skipping'

# Remove old data local
def clean_local(now):
    remove_datetime = now - timedelta(hours=24)
    files = os.listdir('data')
    for f in files:
        if '.html' in f or '.json' in f:
            fp = os.path.join('data', f)
            make_time = datetime.fromtimestamp(os.path.getmtime(fp))
            if make_time < remove_datetime:
                print "Deleting ", fp
                os.remove( fp )

if __name__ == '__main__':
    parser = OptionParser()
    parser.add_option("-a", "--all", action="store_true", dest="all", default=None, help="Run the entire script")
    parser.add_option("-g", "--get", action="store_true", dest="get", default=None, help="Get most recent data")
    parser.add_option("-p", "--process", action="store_true", dest="process", default=None, help="Process most recent data")
    parser.add_option("-c", "--combine", action="store_true", dest="combine", default=None, help="Combine recent data files")
    parser.add_option("-u", "--upload", action="store_true", dest="upload", default=None, help="Upload the recent data files")
    parser.add_option("-d", "--deploy", action="store_true", dest="deploy", default=None, help="Deploy the server")
    parser.add_option("-s", "--cleans3", action="store_true", dest="cleans3", default=None, help="Clean S3")
    parser.add_option("-l", "--cleanlocal", action="store_true", dest="cleanlocal", default=None, help="Clean Local")
    (options, args) = parser.parse_args()

    if options.all:
        options.get = options.process = options.combine = options.upload = options.deploy = True

    now = datetime.now()
    now_15 = now.replace(minute=(now.minute/15)*15)

    filename = os.path.join('data','hn-data-%s.html' % now_15.strftime('%Y-%m-%d-%H-%M'))
    filename_js = filename.replace('.html','.json')

    if options.get:
        print 'Getting HN data'
        get(filename)

    if options.process:
        print 'Processing HN data'
        process(filename, filename_js)

    if options.combine:
        print 'Generating a data file'
        combine(now_15)

    if options.upload:
        print 'Uploading to S3'
        upload(now_15)

    if options.deploy:
        print 'Deploying to S3'
        deploy(dirs = ['.','js','css','img'], exts = ['html', 'js', 'css', 'png', 'json'])

    if options.cleans3:
        print 'Cleaning S3'
        clean_s3(now_15)

    if options.cleanlocal:
        print 'Cleaning Local'
        clean_local(now_15)
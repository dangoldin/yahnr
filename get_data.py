import requests
import json
import re
import os
from datetime import datetime
from bs4 import BeautifulSoup

# Set up Regexes
RE_NUM = re.compile(r'\d+')

now = datetime.now()
now_15 = now.replace(minute=(now.minute/15)*15)
filename = os.path.join('data','hn-data-%s.html' % now_15.strftime('%Y-%m-%d-%H-%M'))
filename_js = filename.replace('.html','.json')

# Retrieve data
f = open(filename,'w')

url = 'http://news.ycombinator.com/news'
r = requests.get(url)
if r.status_code == 200:
    #print r.content
    f.write(r.content)
    f.close()
else:
    print "Could not get HN feed"
    exit()

# Process data
f = open(filename,'r')
soup = BeautifulSoup(f.read())
summary = []
for row in soup.find_all('tr')[2:]:
    if len(row.find_all('td')) == 3:
        cells = row.find_all('td')
        info_row = row.next_sibling
        #print row.text, info_row.text

        order = cells[0].text
        link_data = cells[2]
        title = cells[2].find('a').text
        url = cells[2].find('a')['href']
        domain = cells[2].find('span').text if cells[2].find('span') else ''
        points = info_row.find('span').text
        user = info_row.find('a').text
        num_comments = info_row.find_all('a')[1].text
        thread_id = info_row.find_all('a')[1]['href']

        data = {'order' : RE_NUM.search(order).group(0),
                'title' : title,
                'url'   : url,
                'domain': domain.strip('() '),
                'points': RE_NUM.search(points).group(0),
                'user'  : user,
                'num_comments' : RE_NUM.search(num_comments).group(0) if 'comments' in num_comments else 0,
                'thread_id' : RE_NUM.search(thread_id).group(0),
                }
        summary.append(data)
f.close()

#print json.dumps(summary, indent=2)

o = open(filename_js,'w')
o.write(json.dumps(summary, indent=2))
o.close()
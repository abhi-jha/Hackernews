# -*- coding: utf-8 -*-
"""
Created on Wed Jan 31 15:29:46 2018

@author: abhishek
"""
import time
import peewee
from peewee import *
from datetime import datetime
import requests
import logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
fh = logging.FileHandler('hackernews.log')
fh.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
fh.setFormatter(formatter)
logger.addHandler(fh)

db = MySQLDatabase('hn', user='root', passwd='root')
last_max = ''
class DATA(peewee.Model):
    id = peewee.IntegerField()
    by = peewee.CharField()
    score = peewee.IntegerField()
    time = peewee.DateTimeField()
    title = peewee.CharField()
    type = peewee.CharField()
    url = peewee.CharField()
    class Meta:
        database = db
        db_table = 'DATA'
class MAX(peewee.Model):
    max_val = peewee.IntegerField()
    alive = peewee.IntegerField()
    time = peewee.DateTimeField(default=datetime.now)
    class Meta:
        database = db
        db_table = 'MAX'

def get_stories():
    cursor = db.execute_sql('select * from MAX where `alive` = 1;')
    l = cursor.fetchall()
    db_max_val = l[len(l)-1]
    db_max_val = db_max_val[0]
    #last_max = db_max_val
    current_online_max = requests.get('https://hacker-news.firebaseio.com/v0/maxitem.json').json()

    if int(db_max_val) == int(current_online_max):
        return

    db.execute_sql('UPDATE MAX SET alive = 0;')
    
    logger.info('current_online_max : '+str(current_online_max))
    logger.info('db_max_val : '+str(db_max_val))
    logger.info('Number of records to be fetched : '+str(current_online_max - db_max_val + 1))
    record = MAX(max_val = int(current_online_max), alive = 1)
    record.save()
    for i in range(current_online_max,db_max_val-1, -1): 
        base = 'https://hacker-news.firebaseio.com/v0/item/'+str(i)+'.json'
        r = ''
        try:
            r = requests.get(base).json()
            r = dict(r)
        except:
            logger.info('No story found for : '+str(i))
            continue
        logger.info(i)
        try:
            record = DATA(id = r['id'], by = r['by'], score = r['score'], 
                          time = datetime.fromtimestamp(r['time']), title = r['title'],
                          type = r['type'], url = r['url'])
            record.save(force_insert = True)
        except Exception as e:
            #logger.info(e)
            continue
def update_votes_and_titles_of_existing_records():
    #cursor = db.execute_sql('select id from DATA ORDER BY time')
    cursor = db.execute_sql('SELECT id FROM DATA WHERE time BETWEEN (CURRENT_DATE() - INTERVAL 1 WEEK) AND CURRENT_DATE() order by time;')
    l = cursor.fetchall()
    logger.info('need to update : '+str(len(l))+" number of records")
    for i in l:
        base = 'https://hacker-news.firebaseio.com/v0/item/'+str(i[0])+'.json'
        r = ''
        try:
            r = requests.get(base).json()
            r = dict(r)
        except:
            logger.info('Story deleted for : '+str(i))
            continue
        try:
            record = DATA(id = r['id'], score = r['score'], title = r['title'], url = r['url'])
            record.save()
            logger.info('updated '+str(i[0]))
        except:
            logger.info('story/user deleted')

def start_running():
    while True:
        logger.info('Getting stories')
        get_stories()
        time.sleep(60*60*5) #sleep for five hours
        logger.info('updating')
        update_votes_and_titles_of_existing_records()
if __name__ == '__main__':
    start_running()

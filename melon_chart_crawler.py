import time
import pymysql
from pandas import DataFrame
import pandas as pd
import requests as rs
from bs4 import BeautifulSoup


def getYoutubeUrl(keyword):
        url = 'https://www.youtube.com/results?search_query=' + keyword.encode('utf-8')
        html = rs.get(url).text
        soup = BeautifulSoup(html, 'lxml')

        #temp = soup.find('div', attrs = {'class' : 'yt-lockup-thumbnail contains-addto'}).find('a')['href'].split('=')[1]
        temp = soup.find('div', attrs = {'id' : 'results'})
        t = temp.find('div', attrs = {'class' : 'yt-lockup-dismissable yt-uix-tile'}).find('img')['src'].split('/')[4]
        songUrl = 'https://www.youtube.com/embed/' + t

        return songUrl

def getRealTimeTopRank():
        html = rs.get('http://www.melon.com/chart/').text
        soup = BeautifulSoup(html, 'lxml')

        song = soup.findAll('div', attrs = {'class' : 'ellipsis rank01'})
        artist = soup.findAll('div', attrs = {'class' : 'ellipsis rank02'})

        list_title = []
        list_artist = []
        rank = []
        list_url = []

        idx = 0;
        for s in song:
                title = s.find('a')
                idx += 1
                rank.append(idx)
                temp = getYoutubeUrl(title.text)
                list_url.append(temp)
                list_title.append(title.text)
                
        rank = []
        idx = 0;
        for a in artist:
                title = a.find('a')
                idx += 1
                rank.append(idx)
                list_artist.append(title.text)

        data = {'rank' : rank, 'title' : list_title, 'artist' : list_artist, 'url' : list_url}
        result = DataFrame(data)
        updateChart(result)

#       result.to_csv('~/web_crawler/realTime_top_100.csv', encoding = 'euc-kr')

        print 'complete'


def updateChart(item):


        conn = pymysql.connect(host=' ', user=' ', password=' ', db=' ', charset='utf8')
        cursor = conn.cursor()

        for i in range(0,100):
                temp = item.iloc[i]
                artist = temp[0]
                rank = temp[1]
                title = temp[2]
                url = temp[3]
                print '%s %s %s %s' %(rank, title, artist, url)
                query = 'INSERT INTO admin.realTimeTopRank (rank, title, artist, url) VALUES ("%d", "%s", "%s", "%s") ' %(rank, title, artist, url)
                query2 = 'ON DUPLICATE KEY UPDATE rank = "%d", title = "%s", artist = "%s", url = "%s"' %(rank, title, artist, url)
                cursor.execute(query + query2)
                conn.commit()

        conn.close()
        print 'DB Update Complete'


daemon_flag = True;
def Daemon():

#       while (daemon_flag):
                getRealTimeTopRank();

#               time.sleep(3600)

if __name__ == '__main__':
        Daemon()




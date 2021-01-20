# -*- coding: utf-8 -*-

import os
import sys
import requests
import time
import configparser
import subprocess
import progressbar
from threading import Thread
import threading
import smtplib
from email.mime.text import MIMEText
from email.header    import Header
import sqlite3
from bs4 import BeautifulSoup


WorkDir=os.path.realpath(os.path.dirname(sys.argv[0]))
config = configparser.ConfigParser()
config.read(WorkDir+r'\settings.ini')

kometaPath=config['DEFAULT']['tmpPath']
url=config['DEFAULT']['urlToSend']
urlGet3D=config['DEFAULT']['urlGet3D']
urlGetReport=config['DEFAULT']['urlGetReport']
logFile=config['DEFAULT']['sendLogFile']
resultPath=config['DEFAULT']['resultPath']

host=config['DEFAULT']['smtpHost']
smtpPort=config['DEFAULT']['smtpPort']
username=config['DEFAULT']['login']
password=config['DEFAULT']['psw']
addrToSendMail=config['DEFAULT']['addrToSendMail']
DBPath=config['DEFAULT']['DBPath']
lastMsg=time.time()

def getTime(t1):
    t2=time.time()
    d=divmod(int(t1)-int(t2),86400)
    h = divmod(d[1],3600)
    m = divmod(h[1],60)
    s = m[1]
    return m[0]*60+s

def printTime(running):
    widgets=['[', progressbar.Timer(), '] ',progressbar.Bar(),
                 ' (', progressbar.ETA(), ') ',]
    bar =  progressbar.ProgressBar(maxval=progressbar.UnknownLength,widgets=widgets).start()
    i=0
    while running.is_set():
        time.sleep(0.1)
        bar.update(i)
        i=i+1
    bar.finish()

def send_email(pacId,body_text,path):
    with open(path+"\\output.html", "r") as f:  
        contents = f.read()
        soup = BeautifulSoup(contents, 'lxml')
        tags=[]
        for tag in soup.find_all("h1"):
            tags.append(tag.text)
    body_text=body_text+'\n'+tags[1]+'\n'+tags[2]+'\n'+tags[3]
    msg = MIMEText(body_text, 'plain', 'utf-8')
    subject='Уведомление о выполнении исследования пациента '+str(pacId)
    msg['Subject'] = Header(subject, 'utf-8')
    msg['From'] = username
    msg['To'] = addrToSendMail
    try:
        Server=smtplib.SMTP(host,smtpPort)
        Server.starttls()
        Server.login(username, password)
        Server.sendmail(username, [addrToSendMail], msg.as_string())
        Server.quit()
        print('Отправлено уведомление о пациенте '+str(pacId))
    except:
        print('Ошибка отправки уведомления на почту.')


def find_all(a_str, sub):
    start = 0
    while True:
        start = a_str.find(sub, start)
        if start == -1: return
        yield start
        start += len(sub) # use start += 1 to find overlapping matches

def researchSend(path,DBPath):
    conn = sqlite3.connect(DBPath)
    cursor = conn.cursor()
    sql = """UPDATE Researchings SET
             Sended=1
             WHERE ResearchPath=?
          """
    params=[(path)]
    cursor.execute(sql, params)
    conn.commit()
    conn.close()

def getedAnswer(path,DBPath):
    conn = sqlite3.connect(DBPath)
    cursor = conn.cursor()
    sql = """UPDATE Researchings SET
             GetedAnswer=1
             WHERE ResearchPath=?
          """
    params=[(path)]
    cursor.execute(sql, params)
    conn.commit()
    conn.close()    

def checkResearchSended(path,DBPath):
    conn = sqlite3.connect(DBPath)
    cursor = conn.cursor()
    sql = "SELECT * FROM Researchings WHERE ResearchPath=?"
    cursor.execute(sql, [(path)])
    result=cursor.fetchall() # or use fetchone()
    if len(result)>0:
        if result[0][2]!=1:
            conn.close()
            return False
        else:
            if result[0][3]!=1:
                conn.close()
                print('Исследование отправлено, но по нему не получен ответ. Отправляем заново...')
            if result[0][4]!=1:
                conn.close()
                print('Исследование отправлено, но по нему не получена 3D модель. Отправляем заново...')
                return False
            if result[0][5]!=1:
                conn.close()
                print('Исследование отправлено, но по нему не получен отчет. Отправляем заново...')
                return False
            conn.close()
            print('Исследование отправлено и по нему получены все ответы.')
            return True
    else:
        sql = "INSERT INTO Researchings(ResearchPath) VALUES (?)"
        params=[(path)]
        cursor.execute(sql, params)
        conn.commit()
    conn.close()
    return False

def checkAnswer(path,DBPath):
    conn = sqlite3.connect(DBPath)
    cursor = conn.cursor()
    sql = "SELECT * FROM Researchings WHERE ResearchPath=?"
    cursor.execute(sql, [(path)])
    result=cursor.fetchall() # or use fetchone()
    if len(result)>0:
        if result[0][3]!=1:
            conn.close()
            return False
        else:
            conn.close()
            return True
    conn.close()
    return False

def check3DHTML(path):
    f=open(path)
    data=f.read()
    f.close()
    return data.find('</html>')>0

def check3D(path,DBPath):
    conn = sqlite3.connect(DBPath)
    cursor = conn.cursor()
    sql = "SELECT * FROM Researchings WHERE ResearchPath=?"
    cursor.execute(sql, [(path)])
    result=cursor.fetchall() # or use fetchone()
    if len(result)>0:
        if result[0][4]!=1:
            conn.close()
            return False
        else:
            conn.close()
            return True
    conn.close()
    return False

def checkReport(path,DBPath):
    conn = sqlite3.connect(DBPath)
    cursor = conn.cursor()
    sql = "SELECT * FROM Researchings WHERE ResearchPath=?"
    cursor.execute(sql, [(path)])
    result=cursor.fetchall() # or use fetchone()
    if len(result)>0:
        if result[0][5]!=1:
            conn.close()
            return False
        else:
            conn.close()
            return True
    conn.close()
    return False

def getedReport(path,DBPath):
    conn = sqlite3.connect(DBPath)
    cursor = conn.cursor()
    sql = """UPDATE Researchings SET
             GetedReport=1
             WHERE ResearchPath=?
          """
    params=[(path)]
    cursor.execute(sql, params)
    conn.commit()
    conn.close()  
    
def geted3D(path,DBPath):
    conn = sqlite3.connect(DBPath)
    cursor = conn.cursor()
    sql = """UPDATE Researchings SET
             Geted3D=1
             WHERE ResearchPath=?
          """
    params=[(path)]
    cursor.execute(sql, params)
    conn.commit()
    conn.close() 

with open(logFile,'r') as logF:
    logTxt=logF.read()
    logF.close()

for root, dirs, files in os.walk(kometaPath, topdown = False):
    fileNames=[]
    researchSended=checkResearchSended(root,DBPath)
    if not researchSended:    
        for name in files:
            fileNames.append(('files',open(root+'\\'+name, 'rb')))
    if len(fileNames)>0:       
        try:
            a=list(find_all(root,'\\'))
            pacientId=(root[a[-2]+1:a[-1]])
            resultPathFull=resultPath+'\\'+pacientId
            if not os.path.exists(resultPathFull):
                os.mkdir(resultPathFull)
            resultPathFull=resultPathFull+'\\'+root[root.rfind('\\',2)+1:]
            if not os.path.exists(resultPathFull):
                os.mkdir(resultPathFull)
            ses = requests.Session()
            logTxt=logTxt+'******************************************************************************\n'
            logTxt=logTxt+'Отправка пакета файлов из '+root+'\n'
            print('******************************************************************************')
            response  = ses.get(url, timeout=600)
            print('Отправляем '+root)
            running = threading.Event()
            running.set()
            thread1 = Thread(target=printTime,args=(running,))
            thread1.start()
            response  = ses.post(url, files=fileNames, timeout=600)
            running.clear()
            thread1.join()
            if response.status_code==200:
                researchSend(root,DBPath)
            else:
                print('Не удалось отправить исследование '+root+' Код ответа сервера- '+str(response.status_code))
                continue
            if not checkAnswer(root,DBPath):
                logTxt=logTxt+'Сохраняем ответ отправки'+'\n'
                with open(resultPathFull+'\output.html', 'w') as f:
                    f.write(response.text)
                    f.close()
                getedAnswer(root,DBPath)
            logTxt=logTxt+'Код ответа сервера '+str(response.status_code)+'\n'
            statusCode=0
            if not check3D(root,DBPath):
                logTxt=logTxt+'Получаем 3D модель'+'\n'
                print('Получаем 3D модель')
                response  = ses.get(urlGet3D, timeout=600)
                statusCode=response.status_code
                print('Ответ сервера '+str(statusCode))
                file_size = int(response.headers['Content-Length'])
                chunk = 1
                num_bars = file_size / chunk
                print('Сохраняем 3D модель '+str(file_size/1024)+'Кб')
                logTxt=logTxt+'Сохраняем 3D модель '+str(file_size/1024)+'Кб'+'\n'
                widgets=[' [', progressbar.Timer(), '] ',progressbar.Bar(),
                 ' (', progressbar.ETA(), ') ',]
                bar =  progressbar.ProgressBar(maxval=num_bars,widgets=widgets).start()
                i = 0
                with open(resultPathFull+r'\3D.html', 'wb') as f:
                    for chunk in response.iter_content():
                        f.write(chunk)
                        bar.update(i)
                        i+=1
                    f.close()
                bar.finish() 
                logTxt=logTxt+'Код ответа сервера '+str(response.status_code)+'\n'
                if response.status_code==200:
                    if check3DHTML(resultPathFull+r'\3D.html'):
                        geted3D(root,DBPath)
                    else:
                        print('Файл 3D модели поврежден!')
            if not checkReport(root,DBPath):
                logTxt=logTxt+'Получаем файл отчета'+'\n'
                print('Получаем файл отчета')
                response  = ses.get(urlGetReport, timeout=600)
                statusCode=response.status_code
                print('Ответ сервера '+str(statusCode))
                logTxt=logTxt+'Сохраняем файл отчета'+'\n'
                file_size = int(response.headers['Content-Length'])
                chunk = 1
                num_bars = file_size / chunk
                print('Сохраняем файл отчета '+str(file_size/1024)+'Кб')
                widgets=[' [', progressbar.Timer(), '] ',progressbar.Bar(),
                 ' (', progressbar.ETA(), ') ',]
                bar =  progressbar.ProgressBar(maxval=num_bars,widgets=widgets).start()
                i = 0
                with open(resultPathFull+r'\report.pdf', 'wb') as f:
                    for chunk in response.iter_content():
                        f.write(chunk)
                        bar.update(i)
                        i+=1
                    f.close()
                bar.finish()
                logTxt=logTxt+'Код ответа сервера '+str(response.status_code)+'\n'
                if response.status_code==200:
                    getedReport(root,DBPath)
            
            msg=('Здравствуйте. \n\n Получены отчеты по распознаванию пациента '+pacientId+
                 '.\n Результаты находятся в '+resultPathFull)
            thread1 = Thread(target=send_email,args=(pacientId,msg,resultPathFull,))
            thread1.start()
            thread1.join()
            comand=r'RMDIR  /S /Q "'+os.path.abspath(root)+'"'
            if os.path.exists(root):
                output = subprocess.Popen(comand,shell=True)                
            logTxt=logTxt+'Удаляем временную папку '+root+'\n'
            logTxt=logTxt+'******************************************************************************\n'
        except Exception:
            logTxt=logTxt+'Возникла ошибка обработки'+'\n'
            logTxt=logTxt+str(sys.exc_info()[0])+'\n'
            print(sys.exc_info()[0])
            continue

with open(logFile,'w') as logF:
    logF.write(logTxt)
    logF.close()
print('Отправка завершена....')


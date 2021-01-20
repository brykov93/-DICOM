# -*- coding: utf-8 -*-

import os
import sys
import shutil
import configparser
import  subprocess
from pydicom import dcmread
import progressbar
import time
import sqlite3

def getTime(dirToCheck):
    t1=time.time()
    t2=max(os.path.getmtime(root) for root,_,_ in os.walk(dirToCheck))
    d=divmod(int(t1)-int(t2),86400)
    h = divmod(d[1],3600)
    m = divmod(h[1],60)
    s = m[1]
    return m[0]*60+s


WorkDir=os.path.realpath(os.path.dirname(sys.argv[0]))
config = configparser.ConfigParser()
config.read(WorkDir+r'\settings.ini')
kometaPath=config['DEFAULT']['kometaPath']
tmpPath=config['DEFAULT']['tmpPath']
logFile=config['DEFAULT']['logFile']
folderBlockTimer=config['DEFAULT']['folderBlockTimer']
findSleepTimer=config['DEFAULT']['findSleepTimer']
isslLevel=config['DEFAULT']['isslLevel']
senderPath=config['DEFAULT']['senderPath']
DBPath=config['DEFAULT']['DBPath']
localPath=int(config['DEFAULT']['localPath'])
s=config['DEFAULT']['Stations']
Stations=s.split(';')
stationsParam={}
for station in Stations:
    stationParam={}
    stationParam['StudyDescription']=config[station]['StudyDescription']
    stationParam['Modality']=config[station]['Modality']
    stationParam['SeriesDescription']=config[station]['SeriesDescription']
    stationsParam[station]=stationParam
os.chdir(kometaPath)
needFiles=[]

def get_size(start_path):
    total_size = 0
    for dirpath, dirnames, filenames in os.walk(start_path):
        for f in filenames:
            fp = os.path.join(dirpath, f)
            try:
                total_size += os.path.getsize(fp)
            except:
                print('Не удалось расчитать размер файла '+fp)
    return total_size


def makeDatabase(DBPath):
    conn = sqlite3.connect(DBPath)
    cursor = conn.cursor()
    cursor.execute("""CREATE TABLE FinderCheck
                  (ResearchPath text, 
                  ResearchSize real)
               """)
    conn.commit()
    cursor.execute("""CREATE TABLE FinderBot
                  (FilePathOriginal text,  
                  FilePathAnonimized text, 
                  PacientId integer,
                  Series text, 
                  StudyDate text,
                  StudyTime text, 
                  Ckeced integer, 
                  Copied integer,
                  Anonimized integer, 
                  Cleared integer)
               """)
    conn.commit()
    cursor.execute("""CREATE TABLE Researchings
                  (ResearchPath text, 
                  Series text,
                  Sended integer,
                  GetedAnswer integer,
                  Geted3D integer,
                  GetedReport integer)
               """)
    conn.commit()
    conn.close()


def updateResearchSize(ResearchDir,ResearchSize,DBPath):
    conn = sqlite3.connect(DBPath)
    cursor = conn.cursor()
    sql = "UPDATE FinderCheck SET ResearchSize=? WHERE ResearchPath=?"
    cursor.execute(sql, (ResearchSize,ResearchDir))
    conn.commit()
    conn.close()

def checkReserchInDB(ResearchDir,ResearchSize,DBPath):
    conn = sqlite3.connect(DBPath)
    cursor = conn.cursor()
    sql = "SELECT * FROM FinderCheck WHERE ResearchPath=?"
    cursor.execute(sql, [(ResearchDir)])
    result=cursor.fetchall() # or use fetchone()
    if len(result)>0:
        if result[0][1]!=ResearchSize:
            conn.close()
            updateResearchSize(ResearchDir,ResearchSize,DBPath)
            return False
    else:
        sql = "INSERT INTO FinderCheck VALUES (?,?)"
        params=(ResearchDir,ResearchSize)
        cursor.execute(sql, params)
        conn.commit()
    conn.close()
    return True

def checkFileChecked(path,DBPath):
    conn = sqlite3.connect(DBPath)
    cursor = conn.cursor()
    sql = "SELECT * FROM FinderBot WHERE FilePathOriginal=?"
    cursor.execute(sql, [(path)])
    result=cursor.fetchall() # or use fetchone()
    if len(result)>0:
        conn.close()
        return (result[0][6]==1)
    else:
        conn.close()
        return False
        
def fileChecked(path,DBPath):
    conn = sqlite3.connect(DBPath)
    cursor = conn.cursor()
    sql = "INSERT INTO FinderBot(FilePathOriginal,Ckeced) VALUES (?,?)"
    params=[(path,1)]
    cursor.executemany(sql, params)
    conn.commit()
    conn.close()

def fileCopied(fileInfo,DBPath):
    conn = sqlite3.connect(DBPath)
    cursor = conn.cursor()
    sql = '''UPDATE FinderBot SET
                  PacientId=?,
                  Series=?, 
                  StudyDate=?,
                  StudyTime=?, 
                  Copied=?
             WHERE FilePathOriginal=?'''
    params=[(fileInfo['PatientID'],
             fileInfo['Series'],
             fileInfo['StudyDate'],
             fileInfo['StudyTime'],
             1,
             fileInfo['Path'])]
    cursor.executemany(sql, params)
    conn.commit()
    conn.close()

def fileAnonimized(newFilePath,fileInfo,DBPath):
    conn = sqlite3.connect(DBPath)
    cursor = conn.cursor()
    sql = '''UPDATE FinderBot SET
                  FilePathAnonimized=?,
                  Anonimized=?
             WHERE FilePathOriginal=?'''
    params=[(newFilePath,
             1,
             fileInfo['Path'])]
    cursor.executemany(sql, params)
    conn.commit()
    conn.close()

def checkPath(Path):
    symbols=['>','>',':','"','/','\\','|','?','*']
    result=Path
    for symbol in symbols:
        if symbol in result:
            result=result.replace(symbol,'')
    return result  


if not os.path.exists(DBPath):
    makeDatabase(DBPath)
    
firstStep=True
while True:
    needFiles=[]
    with open(logFile,'r') as logF:
        logTxt=logF.read()
        logF.close()
    i=1
    for root, dirs, files in os.walk(kometaPath, topdown = False):
        i=0
        widgets=[' [', progressbar.Timer(), '] ',progressbar.Bar(),
                 ' (', progressbar.ETA(), ') ',]
        bar = progressbar.ProgressBar(maxval=len(files)+1,widgets=widgets)
        bar.start()
        minPath=os.path.join(kometaPath, root)
        tmp=minPath.split('\\')
        if localPath==0:
            isslDir='\\'.join(tmp[:int(isslLevel)+2])
        else:
            isslDir='\\'.join(tmp[:int(isslLevel)])
        if not checkReserchInDB(isslDir,get_size(isslDir),DBPath):
            print('Исследование загружается, пропускаем...'+isslDir)
            continue   
        if firstStep:
            print('Первый проход! Смотрим дальше...')
            continue
        for name in files:
            i=i+1
            path=os.path.join(kometaPath, root, name)
            filename, file_extension = os.path.splitext(path)
            if file_extension=='':
                if checkFileChecked(path,DBPath):
                    print('Исследование отправлено, пропускаем...'+path)
                    continue
                else:
                    needAppend=False
                    with open(path, 'rb') as infile:
                        ds = dcmread(infile)
                        if 'StationName' in ds:
                            if ds.StationName in stationsParam:
                                StudyDescription=stationsParam[ds.StationName]['StudyDescription']
                                Modality=stationsParam[ds.StationName]['Modality']
                                SeriesDescription=stationsParam[ds.StationName]['SeriesDescription']
                                if (('StudyDescription' in ds and ds.StudyDescription==StudyDescription) and
                                    ('Modality' in ds and ds.Modality==Modality) and
                                    ('SeriesDescription' in ds and ds.SeriesDescription==SeriesDescription)):
                                    finalInfo={}
                                    finalInfo['PatientID']=ds.PatientID
                                    finalInfo['Path']=path
                                    finalInfo['Series']=ds.SeriesInstanceUID
                                    finalInfo['StudyDate']=ds.StudyDate
                                    finalInfo['StudyTime']=ds.StudyTime
                                    needAppend=True
                            else:
                                print('Параметры поиска не определены для станции '+ds.StationName)
                        else:
                            print('Не указано имя станции, пропускаем.')
                        infile.close()
                    fileChecked(path,DBPath)
                    if needAppend:
                        needFiles.append(finalInfo)
                        print('Подходит по тэгам, добавляем в список '+path)

            bar.update(i+1)
        bar.finish()
    if not firstStep:
        print('Поиск завершен, копируем и анонимизируем...')
        for file in needFiles:
            s=checkPath(file['PatientID'])
            fullTempPath=tmpPath+'\\'+s+'\\'
            if not os.path.exists(fullTempPath):
                os.mkdir(fullTempPath)
            fullTempPath=tmpPath+'\\'+s+'\\'+file['Series']
            if not os.path.exists(fullTempPath):
                os.mkdir(fullTempPath)
            try:
                newFile=fullTempPath+'\\'+file['Path'][file['Path'].rfind('\\')+1:]
                shutil.copy(file['Path'],newFile)
                fileCopied(file,DBPath)
                ###################################################################
                ##########################Анонимизатор#############################
                ###################################################################
                with open(file['Path'], 'rb') as infile:
                    ds = dcmread(infile)
                    ds.PatientName=ds.PatientID
                    ds.save_as(newFile)
                    infile.close()
                fileAnonimized(newFile,file,DBPath)
                print('Скопировали и анонимизировали '+newFile)
            except Exception:
                print(sys.exc_info()[0])
                continue
    with open(logFile,'w') as logF:
        logF.write(logTxt)
        logF.close()

    print('Копирование завершено, запускаем отправку....')
    process = subprocess.Popen([senderPath])
    code = process.wait()
    print('Ждем '+findSleepTimer+' секунд и повторяем поиск.')
    time.sleep(int(findSleepTimer))
    firstStep=False
    

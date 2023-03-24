import pytz
import requests
import pandas as pd
from dotenv import load_dotenv
import os
import math
#import signal
#def exit_gracefully(signum, frame):
#    print("Выполнение скрипта завершено.")
#    exit(0)
#signal.signal(signal.SIGALRM, exit_gracefully)
#signal.alarm(3600) # задаем время в секундах

load_dotenv() # Загрузить переменные окружения из файла .env

api_key = os.getenv('API_KEY') # Использовать переменную окружения API_KEY

from telegram.ext import Application, CommandHandler, Job, MessageHandler

df_data = pd.DataFrame(columns=['chat_id', 'gitlab_url', 'project_id', 'access_token', 'branch', 'last_commit'])


from dateutil import parser
from datetime import datetime, timedelta

def add_one_second(timestamp):
    dt = datetime.fromisoformat(timestamp[:-1])  # Преобразуем строку в datetime
    new_dt = dt + timedelta(seconds=1)  # Прибавляем одну секунду
    return new_dt.isoformat() + 'Z'  # Преобразуем обратно в строку и добавляем 'Z'

def convert_time(time_str):
    dt = parser.isoparse(time_str)
    dt_utc = dt.astimezone(pytz.utc)
    return dt_utc.strftime('%Y-%m-%dT%H:%M:%SZ')


def updateData(update, context):
    df_data = pd.read_csv("data.csv")
    df_data.loc[df_data.chat_id == update.effective_chat.id] = \
        [update.effective_chat.id,
         context.chat_data.get("gitlab_url", 'Url not found'),
         context.chat_data.get("project_id", 'Project id not found'),
         context.chat_data.get("access_token", 'Access Token not found'),
         context.chat_data.get("branch", ''),
         context.chat_data.get("last_commit", '')]
    df_data.to_csv("data.csv", index=False)

def getData(chat_id):
    df_data = pd.read_csv("data.csv")
    data = {
        "gitlab_url": df_data.loc[df_data.chat_id == chat_id, 'gitlab_url'].values[0],
        "project_id": df_data.loc[df_data.chat_id == chat_id, 'project_id'].values[0],
        "access_token": df_data.loc[df_data.chat_id == chat_id, 'access_token'].values[0],
        "branch": df_data.loc[df_data.chat_id == chat_id, 'branch'].values[0],
        "last_commit": df_data.loc[df_data.chat_id == chat_id, 'last_commit'].values[0]
    }
    return data
def getAllData():
    df_data = pd.read_csv("data.csv")
    data = []
    for i in range(len(df_data)):
        data.append({
            "chat_id": df_data.loc[i, 'chat_id'],
            "gitlab_url": df_data.loc[i, 'gitlab_url'],
            "project_id": df_data.loc[i, 'project_id'],
            "access_token": df_data.loc[i, 'access_token'],
            "branch": df_data.loc[i, 'branch'],
            "last_commit": df_data.loc[i, 'last_commit']
        })
    return data

def updateLastCommit(chat_id, last_commit):
    df_data = pd.read_csv("data.csv")
    df_data.loc[df_data.chat_id == chat_id, "last_commit"] = last_commit
    df_data.to_csv("data.csv", index=False)


async def setData(update, context):
    df_data = pd.read_csv("data.csv")
    # print(df_data)
    id = update.effective_chat.id
    if id not in df_data.chat_id.values:
        df_data.loc[len(df_data)] = [id, '', '', '', '', '']
        df_data.to_csv("data.csv", index=False)
        return

    tmp = df_data.loc[df_data.chat_id == id, 'gitlab_url'].values[0]
    # print(tmp)
    context.chat_data["gitlab_url"] = tmp
    tmp = df_data.loc[df_data.chat_id == id]['project_id'].values[0]
    # print(tmp)
    context.chat_data["project_id"] = tmp
    tmp = df_data.loc[df_data.chat_id == id]['access_token'].values[0]
    # print(tmp)
    context.chat_data["access_token"] = tmp
    tmp = df_data.loc[df_data.chat_id == id]['branch'].values[0]
    # print(tmp)
    context.chat_data["branch"] = tmp
    tmp = df_data.loc[df_data.chat_id == id]['last_commit'].values[0]
    # print(tmp)
    context.chat_data["last_commit"] = tmp


# получение списка коммитов
def getCommits(gitlab_url, project_id, access_token, branch='', last_commit=''):
    ref = f'{gitlab_url}/api/v4/projects/{project_id}/repository/commits?'
    if isinstance(branch, str) and branch != '':
        ref += 'ref_name=' + str(branch)
    if isinstance(last_commit, str) and last_commit != '':
        ref += '&since=' + add_one_second(convert_time(last_commit))
    ref += '&per_page=10'

    print(ref)
    response = requests.get(ref, headers={'Authorization': f'Bearer {access_token}'})
    commits = response.json()

    return commits




async def setGitlabUrl(update, context):
    await setData(update, context)
    """Usage: /put value"""
    key = "gitlab_url"
    value = update.message.text.partition(' ')[2]
    context.chat_data[key] = value
    updateData(update, context)
    await update.message.reply_text("GitLab url setted to " + value)


async def getUrl(update, context):
    await setData(update, context)
    key = "gitlab_url"
    value = context.chat_data.get(key, 'Url not found')
    await update.message.reply_text(value)


async def setProjectID(update, context):
    await setData(update, context)
    """Usage: /put value"""
    key = "project_id"
    value = update.message.text.partition(' ')[2]
    context.chat_data[key] = value
    updateData(update, context)
    await update.message.reply_text("Project id setted to " + value)


async def getProjectID(update, context):
    await setData(update, context)
    key = "project_id"
    value = context.chat_data.get(key, 'Project id not found')
    await update.message.reply_text(value)


async def setAccessToken(update, context):
    await setData(update, context)
    key = "access_token"
    value = update.message.text.partition(' ')[2]
    context.chat_data[key] = value
    updateData(update, context)
    await update.message.reply_text("Access Token setted to " + value)


async def getAccessToken(update, context):
    await setData(update, context)
    key = "access_token"
    value = context.chat_data.get(key, 'Access Token not found')
    await update.message.reply_text(value)


async def setBranch(update, context):
    await setData(update, context)
    key = "branch"
    value = update.message.text.partition(' ')[2]
    context.chat_data[key] = value
    updateData(update, context)
    await update.message.reply_text("Branch setted to " + value)


async def getBranch(update, context):
    await setData(update, context)
    key = "branch"
    value = context.chat_data.get(key, 'Branch not found')
    await update.message.reply_text(value)


async def get(update, context):
    await setData(update, context)
    gitlab_url = context.chat_data.get("gitlab_url", 'Url not found')
    project_id = context.chat_data.get("project_id", 'Project id not found')
    access_token = context.chat_data.get("access_token", 'Access Token not found')
    branch = context.chat_data.get("branch", '')
    STR = "gitlab url: " + gitlab_url + "\nproject id: " + project_id + "\naccess token: " + access_token
    if branch != '':
        STR += "\nbranch: " + branch
    await update.message.reply_text(STR)


def isAllSetted(context):
    if context.chat_data.get("gitlab_url", 'Url not found') == 'Url not found':
        return False
    if context.chat_data.get("project_id", 'Project id not found') == 'Project id not found':
        return False
    if context.chat_data.get("access_token", 'Access Token not found') == 'Access Token not found':
        return False
    return True


async def getLast10Commits(update, context):
    await setData(update, context)
    if isAllSetted(context):
        gitlab_url = context.chat_data.get("gitlab_url", 'Url not found')
        project_id = context.chat_data.get("project_id", 'Project id not found')
        access_token = context.chat_data.get("access_token", 'Access Token not found')
        branch = context.chat_data.get("branch", '')
        commits = getCommits(gitlab_url, project_id, access_token, branch)
        i = 0
        commitsSTR = ""
        for commit in commits:
            if i == 0:
                updateData(update, context)
            if i == 10:
                break
            i += 1
            commitsSTR += "\n📅 date:    " + commit['committed_date'] + "" + \
                          "\n✍ author:  " + commit['author_name'] + "" + \
                          "\n✉ message: " + commit['message'] + "\n"

            if(commit)


        await update.message.reply_text(commitsSTR)
    else:
        await update.message.reply_text("Not all data setted")


async def getAllCommitsSinceLast(context, app):

    chat_id = context['chat_id']
    gitlab_url = context['gitlab_url']
    project_id = context['project_id']
    access_token = context['access_token']
    branch = context['branch']
    last = context['last_commit']

    try:
        if not isinstance(gitlab_url, str) or not isinstance(project_id, str) or not isinstance(access_token, str):
            return

        commits = getCommits(gitlab_url, project_id, access_token, branch, last)
        i = 0
        print("chat_id " + str(chat_id) + " commits: " + str(len(commits)))
        commitsSTR = ""
        for commit in commits:
            if i == 10:
                break
            i += 1
            commitsSTR += "\n📅 date:    " + commit['committed_date'] + "" + \
                          "\n✍ author:  " + commit['author_name'] + "" + \
                          "\n✉ message: " + commit['message'] + "\n"

        if len(commits) > 0:
            await app.bot.send_message(chat_id=str(chat_id), text=str(commitsSTR))
            updateLastCommit(chat_id, commits[0]['committed_date'])
    except Exception as e:
        print(e)


async def info(update, context):
    await setData(update, context)
    STR = "This bot can get last 10 commits from your gitlab project\n\n"
    STR += "commands:\n"
    STR += "/setGitlabUrl - set gitlab url\n"
    STR += "/getUrl - get gitlab url\n"
    STR += "/setProjectID - set project id\n"
    STR += "/getProjectID - get project id\n"
    STR += "/setAccessToken - set access token\n"
    STR += "/getAccessToken - get access token\n"
    STR += "/setBranch - set branch\n"
    STR += "/getBranch - get branch\n"
    STR += "/get - get all data\n"
    STR += "/getLast10Commits - get last 10 commits\n"
    await update.message.reply_text(STR)

async def getAllCommitsInDB(context):
    data = getAllData()
    for i in data:
        print(i)
        await getAllCommitsSinceLast(i, context.job.data)

def StartJob(app):
    print("start job")

    job = app.job_queue.run_repeating(getAllCommitsInDB, 300, data=app)



if __name__ == '__main__':
    # init or read pandas file
    try:
        df_data = pd.read_csv("data.csv")

    except FileNotFoundError:

        df_data.to_csv("data.csv", index=False)

    application = Application.builder().token(api_key).build()

    application.add_handler(CommandHandler('setGitlabUrl', setGitlabUrl))
    application.add_handler(CommandHandler('getUrl', getUrl))
    application.add_handler(CommandHandler('setProjectID', setProjectID))
    application.add_handler(CommandHandler('getProjectID', getProjectID))
    application.add_handler(CommandHandler('setAccessToken', setAccessToken))
    application.add_handler(CommandHandler('getAccessToken', getAccessToken))
    application.add_handler(CommandHandler('get', get))
    application.add_handler(CommandHandler('getLast10Commits', getLast10Commits))
    application.add_handler(CommandHandler('setBranch', setBranch))
    application.add_handler(CommandHandler('getBranch', getBranch))
    application.add_handler(CommandHandler('info', info))
    StartJob(application)
    application.run_polling()
    # application.stop()

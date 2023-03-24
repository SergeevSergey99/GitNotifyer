import requests
import pandas as pd
from dotenv import load_dotenv
import os

load_dotenv() # Загрузить переменные окружения из файла .env

api_key = os.getenv('API_KEY') # Использовать переменную окружения API_KEY

from telegram.ext import Application, CommandHandler, Job, MessageHandler

df_data = pd.DataFrame(columns=['chat_id', 'gitlab_url', 'project_id', 'access_token', 'branch', 'last_commit'])


def updateData(update, context):
    df_data = pd.read_csv("data.csv")
    df_data.loc[df_data.chat_id == update.effective_chat.id] = \
        [update.effective_chat.id,
         context.chat_data.get("gitlab_url", 'Url not found'),
         context.chat_data.get("project_id", 'Project id not found'),
         context.chat_data.get("access_token", 'Access Token not found'),
         context.chat_data.get("branch", ''),
         context.chat_data.get("last_commit", '2023-01-01T00:00:00.000+00:00')]
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

def updateLastCommit(chat_id, last_commit):
    df_data = pd.read_csv("data.csv")
    df_data.loc[df_data.chat_id == chat_id, "last_commit"] = last_commit
    df_data.to_csv("data.csv", index=False)


async def setData(update, context):
    df_data = pd.read_csv("data.csv")
    # print(df_data)
    id = update.effective_chat.id
    if id not in df_data.chat_id.values:
        df_data.loc[len(df_data)] = [id, '', '', '', '']
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
def getCommits(gitlab_url, project_id, access_token, branch='', last_commit='2023-01-01T00:00:00.000+00:00'):
    ref = f'{gitlab_url}/api/v4/projects/{project_id}/repository/commits'
    if branch != '':
        ref += '?ref_name=' + branch
    ref += '&since=' + last_commit + '&per_page=10'

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
                context.chat_data["last_commit"] = commit['committed_date']
                updateData(update, context)
            if i == 10:
                break
            i += 1
            commitsSTR += "commit id: " + commit['id'] + \
                          "\ndate: " + commit['committed_date'] + \
                          "\nauthor: " + commit['author_name'] + \
                          "\nmessage: " + commit['message'] + "\n"

        await update.message.reply_text(commitsSTR)
    else:
        await update.message.reply_text("Not all data setted")


async def getAllCommitsSinceLast(_context):

    context = _context.job.data
    chat_id = context['chat_id']
    data = getData(chat_id)
    gitlab_url = data['gitlab_url']
    project_id = data['project_id']
    access_token = data['access_token']
    branch = data['branch']
    commits = getCommits(gitlab_url, project_id, access_token, branch, data['last_commit'])
    i = 0
    print("commits: " + str(len(commits)))
    commitsSTR = ""
    for commit in commits:
        if i == 0:
            updateLastCommit(chat_id, commit['committed_date'])
        if i == 10:
            break
        i += 1
        commitsSTR += "commit id: " + commit['id'] + \
                      "\ndate: " + commit['committed_date'] + \
                      "\nauthor: " + commit['author_name'] + \
                      "\nmessage: " + commit['message'] + "\n"

    if len(commits) > 0:
        await _context.bot.send_message(chat_id=chat_id, text=commitsSTR)


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


async def StartJob(update, context):
    await setData(update, context)
    if isAllSetted(context):
        print("start job")
        job_settings = {
            'chat_id': update.message.chat_id,
            'update': update,
        }
        job = context.job_queue.run_repeating(getAllCommitsSinceLast, 600, data=job_settings)
        context.chat_data['job'] = job



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
    application.add_handler(CommandHandler('startJob', StartJob))

    application.run_polling()
    # application.stop()

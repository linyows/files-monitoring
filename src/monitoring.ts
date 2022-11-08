/**
 * Cloud Storage Files Monitoring
 */
import S3, { Config as S3Config } from './s3'
import Slack, { Attachment } from './slack'

export default class Monitoring {
  run (config: Config) {
    const spreadsheetId = config.spreadsheetId
    const s = SpreadsheetApp.openById(spreadsheetId)
    const c = s.getSheetByName('config')
    const b = s.getSheetByName('bucket')

    const startRow = 2
    const startColumn = 1
    const configData = c.getSheetValues(startRow, startColumn, c.getLastRow(), c.getLastColumn())
    const bucketData = b.getSheetValues(startRow, startColumn, b.getLastRow(), b.getLastColumn())

    const configChannelColumn = 0
    const configTimeColumn = 1
    const configBucketColumn = 2
    const configPrefixColumn = 3
    const configLabelColumn = 4

    const bucketNameColumn = 0
    const bucketIdColumn = 1
    const bucketRegionColumn = 2

    const now = new Date()
    const timeLength = 2
    const timeFullLength = 4
    const minStart = 2
    const nowH = `0${now.getHours()}`.slice(-timeLength)
    const nowM = `00${now.getMinutes()}`.slice(-timeLength)

    const tasks = []

    for (const line of configData) {
      const time = `${line[configTimeColumn]}`
      if (time === '') {
        continue
      }
      const hour = time.substr(0, timeLength)
      const min = time.length === timeFullLength ? time.substr(minStart, timeLength) : '00'
      if (hour !== nowH || min !== nowM) {
        continue
      }

      const bucketLine = bucketData.find(v => v[bucketNameColumn] === line[configBucketColumn])
      const accessKeyId = bucketLine[bucketIdColumn]
      const secretAccessKey = PropertiesService.getScriptProperties().getProperty(accessKeyId)
      if (secretAccessKey === '') {
        console.error(`secretAccessKey not founnd with accessKeyId (${accessKeyId})`)
        continue
      }

      const region = bucketLine[bucketRegionColumn]
      const prefxTemplate = line[configPrefixColumn]
      const task = {
        bucket: line[configBucketColumn],
        prefix: buildPrefix(prefxTemplate),
        result: [],
        label: line[configLabelColumn],
        channel: line[configChannelColumn],
        s3: { accessKeyId, secretAccessKey, region },
      } as Task
      tasks.push(task)
    }

    for (const i in tasks) {
      doATask(tasks[i])
    }

    notify(tasks, config.slackSettings)
  }
}

const buildPrefix = (base: string): string => {
  const regex = /{(yesterday|today):(YYYYMMDD|YYYY-MM-DD)}/g
  const templates = base.match(regex)
  let prefix = base

  for (const t of templates) {
    const matched = t.match(/(yesterday|today):(YYYYMMDD|YYYY-MM-DD)/)
    let date = new Date()
    let replaceTo = ''

    switch (matched[1]) {
      case 'yesterday':
        date.setDate(date.getDate() - 1)
        break
    }

    const timeLength = 2
    const yyyy = date.getFullYear()
    const mm = ('00' + (date.getMonth()+1)).slice(-timeLength)
    const dd = ('00' + date.getDate()).slice(-timeLength)

    switch (matched[2]) {
      case 'YYYYMMDD':
        replaceTo = `${yyyy}${mm}${dd}`
        break
      case 'YYYY-MM-DD':
        replaceTo = `${yyyy}-${mm}-${dd}`
        break
    }
    prefix = prefix.replace(t, replaceTo)
  }

  return prefix
}

const newContents = (): Contents => {
  return {
    key: '',
    lastModified: '',
    size: 0,
  }
}

const doATask = (t: Task) => {
  const s3 = new S3(t.s3)
  const querystring = `?list-type=2&prefix=${t.prefix}`
  const res = s3.getObject(t.bucket, querystring)
  const xml = XmlService.parse(res.getDataAsString())
  const elements = xml.getRootElement().getChildren()

  for (const i in elements) {
    if (elements[i].getName() !== 'Contents') {
      continue
    }

    const contents = newContents()
    const els = elements[i].getChildren()

    for (const ii in els) {
      switch (els[ii].getName()) {
        case 'Key':
          contents.key = els[ii].getText()
          break
        case 'LastModified':
          contents.lastModified = els[ii].getText()
          break
        case 'Size':
          contents.size = + els[ii].getText()
          break
      }
    }

    t.result.push(contents)
  }
}

const notify = (tasks: Task[], s: SlackSettings) => {
  const tasksPerChannel = []
  for (const task of tasks) {
    if (task.channel in tasksPerChannel) {
      tasksPerChannel[task.channel].push(task)
    } else {
      tasksPerChannel[task.channel] = [task]
    }
  }

  const slack = new Slack(s.token)
  const consoleUrl = 'https://s3.console.aws.amazon.com/s3/buckets'
  const awsIconUrl = 'https://raw.githubusercontent.com/linyows/files-monitoring/main/misc/amazon.png'

  for (const channel in tasksPerChannel) {
    const attachments = []
    let failure = false
    for (const task of tasksPerChannel[channel]) {
      const success = task.result.length > 0
      if (failure === false && success === false) {
        failure = true
      }
      /* too verbose?
      const fields = []
      for (const content of task.result) {
        fields.push({ title: 'Bucket', value: task.bucket, short: true })
        fields.push({ title: 'Last Modified', value: content.lastModified, short: true })
        fields.push({ title: 'Size', value: content.size, short: true })
      }*/
      attachments.push({
        title: ``,
        color: `${success ? '#36a64f' : '#cc0033'}`,
        text: `${task.label} \`${task.prefix}\`${success ? '' : '\nHmm, file not found!? :thinking:'}`,
        footer: `${task.bucket} on <${consoleUrl}|AWS S3>`,
        footer_icon: awsIconUrl,
      })
    }
    const text = `${s.text}${failure ? ' ' + s.failureMessage : ''}${s.suffixMessage}`
    try {
      slack.postMessage({
        channel,
        username: s.username,
        icon_emoji: s.iconEmoji,
        link_names: 1,
        text,
        attachments: JSON.stringify(attachments),
      })
    } catch (e) {
      console.error(e)
    }
  }
}

export type Config = {
  spreadsheetId: string
  slackSettings: SlackSettings
}

type Task = {
  bucket: string
  prefix: string
  result: Contents[]
  channel: string
  label: string
  s3: S3Config
}

type Contents = {
  key: string
  lastModified: string
  size: number
}

type SlackSettings = {
  token: string
  username: string
  iconEmoji: string
  text: string
  failureMessage: string
  suffixMessage: string
}
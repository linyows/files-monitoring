/**
 * Cloud Storage Files Monitoring
 */
import Monitoring from './monitoring'

/**
 * Main
 */
/* eslint @typescript-eslint/nounused-vars: 0 */
function main() {
  const spreadsheetId = PropertiesService.getScriptProperties().getProperty('SPREADSHEET_ID')
  const token = PropertiesService.getScriptProperties().getProperty('SLACK_TOKEN')
  const sheetUrl = `https://docs.google.com/spreadsheets/d/${spreadsheetId}/edit`
  const projectUrl = 'https://github.com/linyows/files-monitoring'
  const slackSettings = {
    token,
    username: 'Files Monitoring',
    iconEmoji: ':floppy_disk:',
    text: `Hey, This is file monitoring results on cloud storage!`,
    failureMessage: `File does not exist, check it out :point_down:`,
    suffixMessage: ` -- <${sheetUrl}|Settings> | <${projectUrl}|About>`,
  }
  const m = new Monitoring()
  m.run({ spreadsheetId, slackSettings })
}
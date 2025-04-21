import random from 'random';
import { waitFor, logProgress, stop, getHTML, urlExists, isSiteActive } from './utils.js';
import { faRequestHeaders } from './login.js';
import * as db from './database-interface.js';
import fs from 'fs-extra';
import { join } from 'node:path';
import got from 'got';
import { ARTIST_DIR } from './constants.js';

const progressID = 'file';
const dlOptions = {
  mode: 0o770,
};
const maxRetries = 5;
let thumbnailsRunning = false;
let totalThumbnails = 0;
let totalFiles = 0;
let currFile = 0;
let currThumbnail = 0;

function resetTotals() {
  totalThumbnails = 0;
  totalFiles = 0;
  currFile = 0;
  currThumbnail = 0;
  logProgress({ filename: '', reset: true }, progressID);
}
function getTotals() {
  if (!totalFiles && !totalThumbnails)
    return '';
  return `[${currFile + currThumbnail}/${totalFiles + totalThumbnails}]`
}
/**
 * Handles the actual download and progress update for file saving.
 * @param {Object} results Results of a db query
 * @param {String} results.content_url
 * @param {String} results.content_name
 * @returns 
 */
/**
 * Always use: { content_url, content_name, username, subfolder }
 * subfolder is one of: 'gallery', 'favorites', 'scraps'
 */
async function downloadSetup({ content_url, content_name, username, subfolder, retryCount = 0 }) {
  if (stop.now) return false;
  if (!username || typeof username !== "string" || !username.trim()) {
    console.error(`[ERROR] No username provided for content "${content_name}" - skipping file operation.`);
    return Promise.reject(new Error('Username missing for download path'));
  }
  // Set canonical download path
  const downloadLocation = join(ARTIST_DIR, username, subfolder);
  // Check for invalid file types to start
  if (/\.$/.test(content_name)) {
    // File name sanity already handled, no-op here for build sanity.
    return Promise.reject();
  }
  // Check to see if this file even exists by checking the header response
  if (await urlExists(content_url)) {
    console.log(`Downloading: ${content_name}`);
    const fileLocation = join(downloadLocation, content_name);
    await fs.ensureDir(downloadLocation, dlOptions);
    return new Promise((resolve, reject) => {
      const dlStream = got.stream(content_url, {
        ...faRequestHeaders,
        ...{
          timeout: { response: 20000 }
        }
      });
      const fStream = fs.createWriteStream(fileLocation, { flags: 'w+', ...dlOptions });
      dlStream.on('downloadProgress', ({ transferred, total, percent }) => {
        const percentage = Math.round(percent * 100);
        logProgress({ transferred, total, percentage, filename: getTotals() }, progressID);
      })
      .on('error', (error) => {
        logProgress.reset(progressID);
        console.error(`Download failed: ${error.message} for ${content_name}`);
        if (!fStream.closed) fStream.end();
        reject();
      });

      fStream.on('error', (error) => {
          logProgress.reset(progressID);
          console.error(`Could not write file '${content_name}' to system: ${error.message}`);
          reject();
        })
        .on('finish', () => {
          // console.log(`[File] Downloaded: '${content_name}'`, progressID);
          resolve();
        });
      dlStream.pipe(fStream);
    }).catch(() => {
      try {
        fs.removeSync(fileLocation);
      } catch (e) {
        console.error(e);
      }
      // Retry if possible!
      if (retryCount < maxRetries) {
        retryCount++;
        console.log(`[Warn] Download error, retrying...`);
        return downloadSetup({ content_url, content_name, username, subfolder, retryCount });
      }
    });
  } else {
    console.warn(`File not found: '${content_name}'`);
    if (!await isSiteActive()) 
      return Promise.reject(new Error('Site down'));
    else return Promise.reject(new Error('Not found'));
  }
}

  // Move unmoved content
async function reorganizeFiles() {
  const content = await db.getAllUnmovedContentData();
  if (!content.length) return;
  console.log('[Data] Reorganizing files...');
  function getPromise(index) {
    if (index >= content.length) return;
    const { account_name, content_name } = content[index];
    fs.ensureDirSync(join(ARTIST_DIR, account_name, 'gallery'), dlOptions); // Assume old content is gallery
    return fs.move(
      join(ARTIST_DIR, content_name),
      join(ARTIST_DIR, account_name, 'gallery', content_name)
    )
    .then(() => {
      // Set file as moved properly
      return db.setContentMoved(content_name);
    }).catch(() => {
      // Do some fallback to make sure it wasn't already moved?
      if (fs.existsSync(join(ARTIST_DIR, account_name, 'gallery', content_name)))
        return db.setContentMoved(content_name);
      else 
        console.log(`[Warn] File not moved: ${content_name}`);
    });
  }
  let i = 0;
  while (i < content.length) {
    if (stop.now) return;
    await getPromise(i++);
  }
  console.log(`[Data] Files reorganized by user!`);
}

export { reorganizeFiles as cleanupFileStructure };
export async function deleteInvalidFiles() {
  const brokenFiles = await db.getAllInvalidFiles();
  if (brokenFiles.length)
    console.log(`[Warn] There are ${brokenFiles.length} invalid files. Deleting...`);
  for (let i = 0; i < brokenFiles.length; i++) {
    const f = brokenFiles[i];
    const { account_name, content_name, content_url } = f;
    const location = join(ARTIST_DIR, account_name, 'gallery', content_name);
    await fs.remove(location);
    await db.setContentNotSaved(content_url);
  }
}

/**
 * Downloads the specified content.
 * @returns 
 */
export async function downloadSpecificContent(row) {
  if (stop.now) return;
  // Defensive unwrapping and normalization
  let account_name = row.account_name || row.username || "";
  if (!account_name || typeof account_name !== "string" || !account_name.trim()) {
    console.error(`[ERROR] No username/account_name for content "${row.content_name}" - skipping.`);
    return; // Don't attempt any file operation without a real username
  }
  // Always ensure subfolder is set
  let subfolder = 'gallery';
  if (row.is_favorite) subfolder = 'favorites';
  else if (row.is_scrap) subfolder = 'scraps';
  return downloadSetup({
    content_url: row.content_url,
    content_name: row.content_name,
    username: account_name.replace(/\.$/, '._'), 
    subfolder
  })
    .then(() => db.setContentSaved(row.content_url))
    .catch((e) => {
      if (!e) return; // Skip if no real error
      if (/site.down/gi.test(e.message)) {
        stop.now = true;
        return console.log(`[Data] FA appears to be down, stopping all downloads`);
      } else if(/not.found/gi.test(e.message)) {
        db.setContentMissing(row.content_name);
      }
    });
}
/**
 * Downloads the specified thumbnail.
 * @returns 
 */
export async function downloadThumbnail({ thumbnail_url, url:contentUrl, account_name }) {
  if (stop.now) return;
  let content_url = thumbnail_url || '';
  // If blank...
  if (!content_url) {
    // Query the page to get it
    const $ = await getHTML(contentUrl).catch(() => false);
    if (!$) return;
    content_url = $('.page-content-type-text, .page-content-type-music').find('#submissionImg').attr('src') || '';
    if (content_url) content_url = 'https:' + content_url;
  }
  if (!content_url) return;
  const content_name = content_url.split('/').pop();
  // Subfolder for thumbnails stays under 'thumbnail'
  return downloadSetup({
    content_url,
    content_name,
    username: account_name.replace(/\.$/, '._'),
    subfolder: 'thumbnail'
  })
    .then(() => db.setThumbnailSaved(contentUrl, content_url, content_name))
    .catch((e) => {
      if (!e) return; // Skip if no real error
      if (/site.down/gi.test(e.message)) {
        stop.now = true;
        return console.log(`[Data] FA appears to be down, stopping all downloads`);
      } else if(/not.found/gi.test(e.message)) {
        db.setThumbnailMissing(content_url);
      }
    });      
}
/**
 * Gets all download urls and records when they're done.
 * @returns 
 */
async function startContentDownloads() {
  if (stop.now) return;
  let data = await db.getAllUnsavedContent();
  if (!data.length) return;
  totalFiles = data.length;
  currFile = 1;
  let i = 0;
  while (i < data.length) {
    if (stop.now) return;
    await downloadSpecificContent(data[i]);
    if (!thumbnailsRunning) startThumbnailDownloads();
    await waitFor(random.int(2000, 4000));
    i++;
    currFile = i + 1;
  }
  await waitFor(random.int(2000, 4000));
  return startContentDownloads();
}
export async function startUserContentDownloads(data) {
  totalFiles = data.length;
  currFile = 1;
  let i = 0;
  while (i < data.length) {
    if (stop.now) break;
    await downloadSpecificContent(data[i]);
    await waitFor(random.int(2000, 4000));
    i++;
    currFile = i + 1;
  }
  await waitFor();
  resetTotals();
}
async function startThumbnailDownloads() {
  if (stop.now) return thumbnailsRunning = false;
  thumbnailsRunning = true;
  const data = await db.getAllUnsavedThumbnails();
  if (!data.length) return thumbnailsRunning = false;
  totalThumbnails = data.length;
  currThumbnail = 1;
  let i = 0;
  while (i < data.length) {
    if (stop.now) return thumbnailsRunning = false;
    await downloadThumbnail(data[i]);
    await waitFor(random.int(1000, 2500));
    i++;
    currThumbnail = i + 1;
  }
  await waitFor(random.int(2000, 3500));
  return startThumbnailDownloads();
}

async function startAllDownloads() {
  await Promise.all([
    startContentDownloads(),
    startThumbnailDownloads(),
  ]);
  resetTotals();
  return;
}
/**
 * Starts the download loop for all content.
 * @returns 
 */
export async function initDownloads() {
  resetTotals();
  await fs.ensureDir(ARTIST_DIR, dlOptions);
  await waitFor(5000);
  if (stop.now) return;
  console.log('[File] Starting downloads...', progressID);
  return startAllDownloads();
}

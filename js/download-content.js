import random from 'random';
import { waitFor, logProgress, stop, getHTML, urlExists, isSiteActive } from './utils.js';
import { faRequestHeaders } from './login.js';
import * as db from './database-interface.js';
import fs from 'fs-extra';
import { join } from 'node:path';
import got from 'got';
<<<<<<< Updated upstream
import { DOWNLOAD_DIR as downloadDir } from './constants.js';
=======
import { ARTIST_DIR } from './constants.js';

>>>>>>> Stashed changes
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

<<<<<<< Updated upstream
export async function cleanupFileStructure() {
  // Fix folder names
  const names = await db.getAllUsernames();
  names.forEach(({ username, account_name }) => {
    const oldPath = join(downloadDir, username);
    const newPath = join(downloadDir, account_name);
    
    // Rename main user directory if needed
    if (oldPath != newPath && fs.existsSync(oldPath))
      fs.renameSync(oldPath, newPath);
      
    // Also check for and rename favorites directory if it exists
    const oldFavPath = join(downloadDir, username, `${username}_Favorites`);
    const newFavPath = join(downloadDir, account_name, `${account_name}_Favorites`);
    if (fs.existsSync(oldFavPath) && oldFavPath !== newFavPath) {
      fs.ensureDirSync(join(downloadDir, account_name));
      fs.renameSync(oldFavPath, newFavPath);
    }
  });
=======
>>>>>>> Stashed changes
  // Move unmoved content
async function reorganizeFiles() {
  const content = await db.getAllUnmovedContentData();
  if (!content.length) return;
  console.log('[Data] Reorganizing files...');
  function getPromise(index) {
    if (index >= content.length) return;
<<<<<<< Updated upstream
    const { content_name, account_name, username, content_url } = content[index];
    
    // Determine if this is a favorite or user's own content
    return db.getSubmissionOwner(content_url)
      .then(owner => {
        const user = username || account_name;
        const contentOwner = owner?.username || owner?.account_name;
        const isFavorite = contentOwner && contentOwner !== user;
        
        // Determine appropriate target directory
        let targetDir;
        if (isFavorite) {
          targetDir = join(downloadDir, user, `${user}_Favorites`);
        } else {
          targetDir = join(downloadDir, user);
        }
        
        // Ensure target directory exists
        fs.ensureDirSync(targetDir, dlOptions);
        
        // Move the file
        return fs.move(join(downloadDir, content_name), join(targetDir, content_name))
          .then(() => {
            // Set file as moved properly
            return db.setContentMoved(content_name);
          }).catch(() => {
            // Do some fallback to make sure it wasn't already moved?
            if (fs.existsSync(join(targetDir, content_name)))
              return db.setContentMoved(content_name);
            else 
              console.log(`[Warn] File not moved: ${content_name}`);
          });
      })
      .catch(err => {
        console.log(`[Error] Failed to determine content ownership: ${err.message}`);
        // Default to regular user directory if ownership can't be determined
        const user = username || account_name;
        const targetDir = join(downloadDir, user);
        fs.ensureDirSync(targetDir, dlOptions);
        
        return fs.move(join(downloadDir, content_name), join(targetDir, content_name))
          .then(() => db.setContentMoved(content_name))
          .catch(e => console.log(`[Warn] File move failed: ${e.message}`));
      });
=======
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
>>>>>>> Stashed changes
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
<<<<<<< Updated upstream
    const { account_name, content_name, content_url, username } = f;
    
    // Check in regular user directory
    const user = username || account_name;
    const userDir = join(downloadDir, user);
    const regularLocation = join(userDir, content_name);
    
    // Check in favorites directory
    const favoritesDir = join(userDir, `${user}_Favorites`);
    const favoritesLocation = join(favoritesDir, content_name);
    
    // Try to remove from both possible locations
    if (fs.existsSync(regularLocation)) {
      await fs.remove(regularLocation);
    }
    
    if (fs.existsSync(favoritesLocation)) {
      await fs.remove(favoritesLocation);
    }
    
    // Mark as not saved in database
=======
    const { account_name, content_name, content_url } = f;
    const location = join(ARTIST_DIR, account_name, 'gallery', content_name);
    await fs.remove(location);
>>>>>>> Stashed changes
    await db.setContentNotSaved(content_url);
  }
}

/**
 * Downloads the specified content.
 * @returns 
 */
<<<<<<< Updated upstream
/**
 * Checks if a file already exists locally to avoid redownloading
 * @param {String} username User who owns the content
 * @param {String} content_name Filename to check
 * @param {Boolean} isFavorite Whether this is a favorite or user's own content
 * @returns {Boolean} True if file exists, false otherwise
 */
/**
/**
 * Checks if a file already exists locally to avoid redownloading
 * @param {String} username User who owns the content
 * @param {String} content_name Filename to check
 * @returns {Boolean} True if file exists, false otherwise
 */
async function fileExistsLocally(username, content_name) {
  
  // Normalize username for file system
  const normalizedUsername = username.replace(/\.$/, '._');
  const userDir = join(downloadDir, normalizedUsername);
  
  // Check in user's regular directory first
  let regularFilePath = join(userDir, content_name);
  if (fs.existsSync(regularFilePath)) {
    console.log(`[Data] File found in user directory: ${content_name}`);
    return true;
  }
  
  // Also check 'thumbnail' subdirectory in case it's a thumbnail
  let thumbnailPath = join(userDir, 'thumbnail', content_name);
  if (fs.existsSync(thumbnailPath)) {
    console.log(`[Data] Thumbnail found in user directory: ${content_name}`);
    return true;
  }
  
  // If it's a favorite or if we're uncertain, check the favorites folder too
  const favoritesDir = join(userDir, `${normalizedUsername}_Favorites`);
  let favoriteFilePath = join(favoritesDir, content_name);
  if (fs.existsSync(favoriteFilePath)) {
    console.log(`[Data] File found in favorites directory: ${content_name}`);
    return true;
  }
  
  // Check thumbnail in favorites
  let favThumbnailPath = join(favoritesDir, 'thumbnail', content_name);
  if (fs.existsSync(favThumbnailPath)) {
    console.log(`[Data] Thumbnail found in favorites directory: ${content_name}`);
    return true;
  }
  
  return false;
}
/**
 * Determines the appropriate download location based on content ownership
 * @param {String} username Username of the downloader
 * @param {String} content_owner Username of the content owner
 * @returns {String} Path where the content should be saved
 */
function getDownloadLocation(username, content_owner) {
  if (!username) {
    console.log('[Error] Missing username for download location');
    return join(downloadDir, 'unknown');
  }
  
  // Normalize usernames for comparison
  const normalizedUser = username.replace(/\.$/, '._').toLowerCase();
  const normalizedOwner = content_owner?.replace(/\.$/, '._').toLowerCase();
  
  // Create the base user directory
  const userDir = join(downloadDir, normalizedUser);
  
  // Ensure the user directory exists
  fs.ensureDirSync(userDir, dlOptions);
  
  // If the content is from the user's own gallery or account name
  if (!content_owner || normalizedUser === normalizedOwner) {
    return userDir;
  }
  
  // Otherwise, it's a favorite from another user
  const favoritesDir = join(userDir, `${normalizedUser}_Favorites`);
  fs.ensureDirSync(favoritesDir, dlOptions);
  return favoritesDir;
}

/**
 * Downloads the specified content.
 * @param {Object} params Download parameters
 * @param {String} params.content_url URL of the content to download
 * @param {String} params.content_name Filename of the content
 * @param {String} params.account_name Account name
 * @param {String} params.username Username 
 * @param {String} params.content_owner Owner of the content (if a favorite)
 * @returns {Promise}
 */
export async function downloadSpecificContent({ content_url, content_name, account_name, username, content_owner }) {
  if (stop.now) return;
  
  if (!content_url || !content_name) {
    console.log(`[Error] Invalid content data: ${content_url} / ${content_name}`);
    return;
  }
  
  // Use provided username or account_name as fallback
  const user = username || account_name;
  if (!user) {
    console.log(`[Error] Missing user information for download: ${content_name}`);
    return;
  }
  
  // Check if file already exists locally
  if (await fileExistsLocally(user, content_name)) {
    console.log(`[Data] File already exists: ${content_name}`);
    await db.setContentSaved(content_url);
    return;
  }
  
  // Calculate the appropriate download location
  const downloadLocation = getDownloadLocation(user, content_owner);
  
  return downloadSetup({ content_url, content_name, downloadLocation })
=======
export async function downloadSpecificContent({ content_url, content_name, account_name, is_favorite, is_scrap }) {
  if (stop.now) return;
  // Always ensure subfolder is set
  let subfolder = 'gallery';
  if (is_favorite) subfolder = 'favorites';
  else if (is_scrap) subfolder = 'scraps';
  return downloadSetup({
    content_url, 
    content_name, 
    username: account_name.replace(/\.$/, '._'), 
    subfolder
  })
>>>>>>> Stashed changes
    .then(() => db.setContentSaved(content_url))
    .catch((e) => {
      if (!e) return; // Skip if no real error
      if (/site.down/gi.test(e.message)) {
        stop.now = true;
        return console.log(`[Data] FA appears to be down, stopping all downloads`);
      } else if(/not.found/gi.test(e.message)) {
        db.setContentMissing(content_name);
      }
    });
}
/**
 * Downloads the specified thumbnail.
 * @returns 
 */
export async function downloadThumbnail({ thumbnail_url, url:contentUrl, account_name, username, content_owner }) {
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
<<<<<<< Updated upstream
  
  // Use provided username or account_name as fallback
  const user = username || account_name;
  
  // Calculate the base download location
  let baseLocation;
  if (content_owner && content_owner !== user) {
    // It's a favorite from another user
    baseLocation = join(downloadDir, user.replace(/\.$/, '._'), `${user}_Favorites`);
  } else {
    // It's user's own content
    baseLocation = join(downloadDir, user.replace(/\.$/, '._'));
  }
  
  const downloadLocation = join(baseLocation, 'thumbnail');
  return downloadSetup({ content_url, content_name, downloadLocation })
=======
  // Subfolder for thumbnails stays under 'thumbnail'
  return downloadSetup({
    content_url,
    content_name,
    username: account_name.replace(/\.$/, '._'),
    subfolder: 'thumbnail'
  })
>>>>>>> Stashed changes
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
  
  // Get content ownership info for determining favorites vs. own content
  for (let i = 0; i < data.length; i++) {
    const item = data[i];
    // Set content_owner to determine if it's a favorite or user's own content
    const submission = await db.getSubmissionOwner(item.content_url);
    if (submission) {
      item.content_owner = submission.username || submission.account_name;
    }
  }
  
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
  
  // Get content ownership info for determining favorites vs. own content
  for (let i = 0; i < data.length; i++) {
    const item = data[i];
    // Set content_owner to determine if it's a favorite or user's own content
    const submission = await db.getSubmissionOwner(item.content_url);
    if (submission) {
      item.content_owner = submission.username || submission.account_name;
    }
  }
  
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
  
  // Get content ownership info for determining favorites vs. own content
  for (let i = 0; i < data.length; i++) {
    const item = data[i];
    // Set content_owner to determine if it's a favorite or user's own content
    const submission = await db.getSubmissionOwner(item.url);
    if (submission) {
      item.content_owner = submission.username || submission.account_name;
    }
  }
  
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

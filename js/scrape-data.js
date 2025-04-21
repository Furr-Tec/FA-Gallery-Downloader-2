import random from 'random';
import { FA_URL_BASE } from './constants.js';
import * as db from './database-interface.js';
import { logProgress, waitFor, getHTML, stop, sendStartupInfo } from './utils.js';
import fs from 'fs-extra';
import { join } from 'node:path';
const scrapeID = 'scrape-div';
const progressID = 'data';
const maxRetries = 6;

/**
 * Walks the user's gallery in order to gather all submission links for future download.
 * @param {String} url Gallery URL
 * @param {Boolean} isScraps Is this the scraps folder or not?
 */
/**
 * Checks if a submission already exists in the database to avoid duplicate downloads
 * @param {String} submissionUrl The URL of the submission to check
 * @param {String} username The username downloading the content
 * @returns {Promise<Boolean>} True if this submission is already processed
 */
async function isSubmissionProcessed(submissionUrl, username) {
  if (!submissionUrl) return false;
  
  // Ensure db is initialized
  if (!db) {
    console.error('Database is not initialized');
    return false;
  }
  
  try {
    // Use getSubmissionOwner function to check if submission exists in database
    const existingSubmission = await db.getSubmissionOwner(submissionUrl);
    
    if (!existingSubmission) {
      return false;
    }
    
    // Need to check if content is saved
    // Since we don't have direct access to is_content_saved in getSubmissionOwner,
    // we'll need to do an additional check
    try {
      const submissions = await db.getAllSubmissionsForUser(existingSubmission.username || existingSubmission.account_name);
      const matchingSubmission = submissions.find(sub => 
        sub.url === submissionUrl || sub.content_url === submissionUrl
      );
      
      // If we don't have this submission in the database, it's not processed
      if (!matchingSubmission) return false;
      
      // If it's in the database but content isn't saved yet, it's not fully processed
      if (!matchingSubmission.is_content_saved) return false;
    
      // Check if the actual file exists on disk
      if (matchingSubmission.content_name && username) {
        const userDir = join(downloadDir, username.replace(/\.$/, '._'));
        const regularPath = join(userDir, matchingSubmission.content_name);
        const favoritesPath = join(userDir, `${username}_Favorites`, matchingSubmission.content_name);
        
        // If file exists in either location, it's processed
        if (fs.existsSync(regularPath) || fs.existsSync(favoritesPath)) {
          return true;
        }
      }
    } catch (innerError) {
      console.error('Error checking submission status:', innerError);
      // Continue with the process even if this check fails
    }
  } catch (error) {
    console.error('Error checking if submission is processed:', error);
    return false;
  }
  
  return false;
}

export async function getSubmissionLinks({ url, username, isScraps = false, isFavorites = false }) {
  let dirName = (isFavorites) ? 'favorites': (isScraps) ? 'scraps' : 'gallery';
  const baseDir = join('fa_gallery_downloader', username.toLowerCase(), dirName);
  await fs.ensureDir(baseDir); // Ensure directory exists
  const divID = `${scrapeID}${isScraps ? '-scraps':''}`;
  let currPageCount = 1;
  let currLinks = 0;
  let newLinks = 0;
  let stopLoop = false;
  let nextPage = ''; // Only valid if in favorites!
  console.log(`[Data] Searching user ${dirName} for submission links...`, divID);
  logProgress.busy(progressID);
  let retryCount = 0;
  while(!stopLoop && !stop.now) {
    const pageUrl = (!nextPage) ? url + currPageCount : nextPage;
    let $ =  await getHTML(pageUrl).catch(() => false);
    if (!$) {
      retryCount++;
      if (retryCount < maxRetries) {
        console.log(`[Warn] FA might be down, retrying in ${30 * retryCount} seconds`);
        await waitFor(30 * retryCount * 1000);
        continue;
      } else {
        stop.now = true;
        return console.log(`[Warn] FA might be down, please try again later`);
      }
    }
    retryCount = 0;
    // Check for content
    let scrapedLinks = Array.from($('figcaption a[href^="/view"]'))
      .map((div) => FA_URL_BASE + div.attribs.href);
      
    if (!scrapedLinks.length) {
      // console.log(`[Data] Found ${currPageCount} pages of submissions!`, divID);
      break;
    }
    
    // Filter out submissions that are already processed
    let filteredLinks = [];
    for (const link of scrapedLinks) {
      if (!(await isSubmissionProcessed(link, username))) {
        filteredLinks.push(link);
      }
    }
    
    // Update counters
    newLinks += filteredLinks.length;
    
    // If we're filtering and have no new links, still count the page but process next
    if (filteredLinks.length === 0) {
      console.log(`[Data] No new submissions found on page ${currPageCount}, checking next page...`);
      currPageCount++;
      if (isFavorites) {
        nextPage = $(`.pagination a.right`).attr('href');
        if (nextPage) nextPage = url.split('/favorite')[0] + nextPage;
        else break;
      }
      await waitFor(random.int(1000, 2500));
      continue;
    }
    
    // Save the filtered links to the database
    await db.saveLinks(filteredLinks, isScraps, username).catch(() => stopLoop = true);
    if (stopLoop || stop.now) {
      console.log('[Data] Stopped early!');
      logProgress.reset(progressID);
      break;
    }
    
    // For favorites, save the relationship between user and submission
    if (isFavorites && username) {
      await db.saveFavorites(username, filteredLinks);
      // Also set a flag to indicate these are favorites
      for (const link of filteredLinks) {
        try {
          // Mark in database this is a favorite of username
          await db.saveMetaData(link, {
            is_favorite: 1,
            favorite_username: username
          });
        } catch (error) {
          console.log(`[Error] Failed to mark favorite: ${error.message}`);
        }
      }
    }
    currLinks += newLinks;
    currPageCount++;
    if (isFavorites) {
      nextPage = $(`.pagination a.right`).attr('href');
      if (nextPage) nextPage = url.split('/favorite')[0] + nextPage;
      else break;
    }
    await waitFor(random.int(1000, 2500));
  }
  if (!stop.now) {
    const skippedLinks = currLinks - newLinks;
    console.log(`[Data] ${currLinks} submissions found, ${newLinks} new submissions to download, ${skippedLinks} already downloaded`);
  }
  logProgress.reset(progressID);
  await sendStartupInfo();
}
/**
 * Gathers and saves the comments from given HTML or url.
 * @param {Cheerio} $ 
 * @param {String} submission_id 
 * @param {String} url 
 */
export async function scrapeComments($, submission_id, url) {
  if (stop.now) return logProgress.reset(progressID);
  let retryCount = 0;
  do {
    $ = $ || await getHTML(url).catch(() => false);
    if (!$) {
      retryCount++;
      if (retryCount < maxRetries) {
        console.log(`[Warn] FA might be down, retrying in ${30 * retryCount} seconds`);
        await waitFor(30 * retryCount * 1000);
        continue;
      } else {
        return console.log(`[Data] Comment page not found: ${url}`);
      }
    }
    break;
  } while (!$);
  const comments = Array.from($('#comments-submission .comment_container'))
    .map((val) => {
      const $div = $(val);
      const isDeleted = $div.find('comment-container').hasClass('deleted-comment-container');
      let date = '';
      if (!isDeleted) {
        date = $div.find('comment-date > span').attr('title').trim();
        if (/ago/i.test(date)) date = $div.find('comment-date > span').text().trim();
      }
      const username = isDeleted ? '' : $div.find('comment-username').text().trim();
      return {
        id: $div.find('.comment_anchor').attr('id'),
        submission_id,
        width: $div.attr('style'),
        username,
        account_name: username.replace(/_/gi, ''),
        desc: isDeleted ? '' : $div.find('comment-user-text .user-submitted-links').html().trim(),
        subtitle: isDeleted ? '' : $div.find('comment-title').text().trim(),
        date,
      }
    });
  if(!comments.length) return;
  return db.saveComments(comments);
}
const metadataID = 'scrape-metadata';
/**
 * Gathers all of the relevant metadata from all uncrawled submission pages.
 * @returns 
 */
export async function scrapeSubmissionInfo({ data = null, downloadComments }) {
  let links = data || await db.getSubmissionLinks();
  if (!links.length || stop.now) return logProgress.reset(progressID);
  console.log(`[Data] Saving data for ${links.length} submissions...`, metadataID);
  let index = 0;
  let retryCount = 0;
  while (index < links.length && !stop.now) {
    logProgress({transferred: index+1, total: links.length}, progressID);
    let $ = await getHTML(links[index].url)
    .then(_$ => {
      if (!_$ || !_$('.submission-title').length) {
        if(_$('.section-body').text().includes('The submission you are trying to find is not in our database.')) {
          console.log(`[Error] Confirmed deleted, removing: ${links[index].url}`);
          db.deleteSubmission(links[index].url);
        } else {
          console.log(`[Error] Not found/deleted: ${links[index].url}`);
        }
        return false;
      } else {
        return _$;
      }
    })
    .catch(() => {
      return false;
    });
    if (!$) {
      retryCount++;
      if (retryCount < maxRetries / 2) {
        console.log(`[Warn] FA might be down, retrying in ${30 * retryCount} seconds`);
        await waitFor(30 * retryCount * 1000);
        continue;
      } else {
        retryCount = 0;
        index++;
        await waitFor(random.int(2000, 3500));
        continue;
      }
    }
    retryCount = 0;
    // Get data if page exists
    let date = $('.submission-id-sub-container .popup_date').attr('title').trim();
    if (/ago$/i.test(date)) date = $('.submission-id-sub-container .popup_date').text().trim();
    const username = $('.submission-title + a').text().trim().toLowerCase();
    const data = {
      id: links[index].url.split('view/')[1].split('/')[0],
      title: $('.submission-title').text().trim(),
      username,
      account_name: username.replace(/_/gi, ''),
      desc: $('.submission-description').html().trim(),
      tags: $('.tags-row').text().match(/([A-Z])\w+/gmi)?.join(','),
      content_name: $('.download > a').attr('href').split('/').pop(),
      content_url: $('.download > a').attr('href'),
      date_uploaded: date,
      thumbnail_url: $('.page-content-type-text, .page-content-type-music').find('#submissionImg').attr('src') || '',
      rating: $('.rating .rating-box').first().text().trim(),
      category: $('.info.text > div > div').text().trim(),
      // Track ownership info for proper file organization
      content_owner: username,
    };
    // Test to fix FA url weirdness
    if (!/^https/i.test(data.content_url)) data.content_url = 'https:' + data.content_url;
    if (data.thumbnail_url && !/^https/i.test(data.thumbnail_url))
      data.thumbnail_url = 'https:' + data.thumbnail_url;
    // Save data to db
    await db.saveMetaData(links[index].url, data);
    // Save comments
    if (downloadComments) await scrapeComments($, data.id);
    index++;
    if (index % 2) await waitFor(random.int(1000, 2500));
  }
  if (!stop.now) console.log('[Data] All submission metadata saved!');
  logProgress.reset(progressID);
}

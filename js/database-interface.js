import sqlite3 from 'sqlite3';
import * as sqlite from 'sqlite';
import fs from 'fs-extra';
import process from 'node:process';
import { DB_LOCATION as dbLocation } from './constants.js';
import { upgradeDatabase } from './database-upgrade.js';

const { open } = sqlite;

// Define database connection state
/**@type {sqlite.Database} */
let db = null;
let isConnecting = false;
let connectionError = null;

// Connection pool configuration
const DB_CONNECTION_CONFIG = {
  max: 20,               // Maximum number of connections in the pool
  min: 2,                // Minimum number of connections to keep idle
  idleTimeoutMillis: 30000,  // How long a connection is allowed to be idle before being closed
  acquireTimeoutMillis: 15000, // Maximum time to wait when acquiring a connection
  createTimeoutMillis: 30000,  // Maximum time to spend trying to create a connection
  maxUses: 7500,         // Maximum number of uses before a connection is closed and a new one created
  reapIntervalMillis: 1000, // How frequently to check for idle connections to close
  createRetryIntervalMillis: 200 // Time between connection creation retry attempts
};

// Track connection status
const connectionStatus = {
  isOpen: false,
  lastError: null,
  lastConnectTime: null,
  reconnectAttempts: 0,
  maxReconnectAttempts: 5
};

// Define logging levels
const LOG_LEVELS = {
  ERROR: 'ERROR',
  WARN: 'WARN',
  INFO: 'INFO',
  DEBUG: 'DEBUG'
};

// Configure logging (can be moved to a separate logger module)
const logLevel = process.env.NODE_ENV === 'production' ? LOG_LEVELS.ERROR : LOG_LEVELS.DEBUG;

/**
 * Structured logger for database operations
 * @param {string} level - Log level
 * @param {string} message - Log message
 * @param {Object} [data] - Additional data to log
 */
function dbLogger(level, message, data = {}) {
  if ((level === LOG_LEVELS.ERROR) || 
      (level === LOG_LEVELS.WARN && logLevel !== LOG_LEVELS.ERROR) ||
      (level === LOG_LEVELS.INFO && (logLevel === LOG_LEVELS.INFO || logLevel === LOG_LEVELS.DEBUG)) ||
      (level === LOG_LEVELS.DEBUG && logLevel === LOG_LEVELS.DEBUG)) {
    
    // Sanitize any sensitive data before logging
    const sanitizedData = { ...data };
    if (sanitizedData.params) {
      // Truncate large param values for readability
      sanitizedData.params = sanitizedData.params.map(p => 
        typeof p === 'string' && p.length > 100 ? p.substring(0, 100) + '...' : p
      );
    }
    
    const logEntry = {
      timestamp: new Date().toISOString(),
      level,
      module: 'database',
      message,
      ...sanitizedData
    };
    
    // In production, we might want to use a proper logging service
    if (level === LOG_LEVELS.ERROR) {
      console.error('[Database]', JSON.stringify(logEntry));
    } else if (level === LOG_LEVELS.WARN) {
      console.warn('[Database]', JSON.stringify(logEntry));
    } else {
      console.log('[Database]', JSON.stringify(logEntry));
    }
  }
}

// INSERT/UPDATE functions
/**
 * Validates input data before database operations
 * @param {any} data - The data to validate
 * @param {string} expectedType - The expected type
 * @returns {boolean} - Whether the data is valid
 */
function validateInput(data, expectedType) {
  if (data === undefined || data === null) {
    return false;
  }
  
  switch (expectedType) {
    case 'string':
      return typeof data === 'string';
    case 'number':
      return typeof data === 'number' && !isNaN(data);
    case 'boolean':
      return typeof data === 'boolean';
    case 'array':
      return Array.isArray(data);
    case 'object':
      return typeof data === 'object' && !Array.isArray(data) && data !== null;
    default:
      return true;
  }
}

/**
 * Generic insert function with proper parameterization
 * @param {string} table - The table name
 * @param {string} columns - Comma-separated column names
 * @param {string[]} placeholders - Array of placeholder groups for values
 * @param {any[]} data - Array of values to insert
 * @returns {Promise<sqlite.RunResult>} - The result of the insert operation
 */
async function genericInsert(table, columns, placeholders, data) {
  try {
    if (!validateInput(table, 'string') || !validateInput(columns, 'string') || 
        !validateInput(placeholders, 'array') || !validateInput(data, 'array')) {
      throw new Error('Invalid input data for database insert');
    }
    
    const query = `INSERT INTO ${table} (${columns}) VALUES ${placeholders.join(',')}`;
    dbLogger(LOG_LEVELS.DEBUG, 'Executing insert query', { table, params: data });
    
    return await db.run(query, ...data);
  } catch (error) {
    dbLogger(LOG_LEVELS.ERROR, 'Error in genericInsert', { 
      table, 
      error: error.message, 
      stack: error.stack,
      params: data 
    });
    throw error;
  }
}

/**
 * Deletes a submission from the database
 * @param {string} url - The URL of the submission to delete
 * @returns {Promise<void>} - Result of the operation
 */
export async function deleteSubmission(url) {
  try {
    if (!validateInput(url, 'string')) {
      throw new Error('Invalid URL for deletion');
    }
    
    dbLogger(LOG_LEVELS.INFO, 'Deleting submission', { url });
    
    return await db.run(`DELETE FROM subdata WHERE url = ?`, [url]);
  } catch (error) {
    dbLogger(LOG_LEVELS.ERROR, 'Error deleting submission', { 
      url, 
      error: error.message,
      stack: error.stack
    });
    throw error;
  }
}
/**
 * Marks the given content_url as saved (downloaded).
 * @param {string} content_url - URL of the content
 * @returns {Promise<sqlite.RunResult>} - Result of the update operation
 */
export async function setContentSaved(content_url) {
  try {
    if (!validateInput(content_url, 'string')) {
      throw new Error('Invalid content URL for saving status update');
    }
    
    dbLogger(LOG_LEVELS.DEBUG, 'Setting content as saved', { content_url });
    
    return await db.run(`
      UPDATE subdata
      SET
        is_content_saved = 1,
        moved_content = 1
      WHERE content_url = ?
    `, [content_url]);
  } catch (error) {
    dbLogger(LOG_LEVELS.ERROR, 'Error setting content as saved', { 
      content_url, 
      error: error.message,
      stack: error.stack
    });
    throw error;
  }
}
/**
 * Marks the given content_url as not saved (invalid file).
 * @param {string} content_url - URL of the content
 * @returns {Promise<sqlite.RunResult>} - Result of the update operation
 */
export async function setContentNotSaved(content_url) {
  try {
    if (!validateInput(content_url, 'string')) {
      throw new Error('Invalid content URL for not-saved status update');
    }
    
    dbLogger(LOG_LEVELS.DEBUG, 'Setting content as not saved', { content_url });
    
    return await db.run(`
      UPDATE subdata
      SET
        is_content_saved = 0,
        moved_content = 1
      WHERE content_url = ?
    `, [content_url]);
  } catch (error) {
    dbLogger(LOG_LEVELS.ERROR, 'Error setting content as not saved', { 
      content_url, 
      error: error.message,
      stack: error.stack
    });
    throw error;
  }
}
/**
 * Marks content as moved to its proper location
 * @param {string} content_name - Name of the content file
 * @returns {Promise<sqlite.RunResult>} - Result of the update operation
 */
export async function setContentMoved(content_name) {
  try {
    if (!validateInput(content_name, 'string')) {
      throw new Error('Invalid content name for moved status update');
    }
    
    dbLogger(LOG_LEVELS.DEBUG, 'Setting content as moved', { content_name });
    
    return await db.run(`
      UPDATE subdata
      SET
        moved_content = 1
      WHERE content_name = ?
    `, [content_name]);
  } catch (error) {
    dbLogger(LOG_LEVELS.ERROR, 'Error setting content as moved', { 
      content_name, 
      error: error.message,
      stack: error.stack
    });
    throw error;
  }
}
/**
 * Marks content as missing (unable to download)
 * @param {string} content_name - Name of the content file
 * @returns {Promise<sqlite.RunResult>} - Result of the update operation
 */
export async function setContentMissing(content_name) {
  try {
    if (!validateInput(content_name, 'string')) {
      throw new Error('Invalid content name for missing status update');
    }
    
    dbLogger(LOG_LEVELS.DEBUG, 'Setting content as missing', { content_name });
    
    return await db.run(`
      UPDATE subdata
      SET
        content_missing = 1
      WHERE content_name = ?
    `, [content_name]);
  } catch (error) {
    dbLogger(LOG_LEVELS.ERROR, 'Error setting content as missing', { 
      content_name, 
      error: error.message,
      stack: error.stack
    });
    throw error;
  }
}
/**
 * Marks thumbnail as missing (unable to download)
 * @param {string} thumbnail_url - URL of the thumbnail
 * @returns {Promise<sqlite.RunResult>} - Result of the update operation
 */
export async function setThumbnailMissing(thumbnail_url) {
  try {
    if (!validateInput(thumbnail_url, 'string')) {
      throw new Error('Invalid thumbnail URL for missing status update');
    }
    
    dbLogger(LOG_LEVELS.DEBUG, 'Setting thumbnail as missing', { thumbnail_url });
    
    return await db.run(`
      UPDATE subdata
      SET
        thumbnail_missing = 1
      WHERE thumbnail_url = ?
    `, [thumbnail_url]);
  } catch (error) {
    dbLogger(LOG_LEVELS.ERROR, 'Error setting thumbnail as missing', { 
      thumbnail_url, 
      stack: error.stack
    });
    throw error;
  }
}
/**
 * Marks thumbnail as saved and updates thumbnail information
 * @param {string} url - Submission URL
 * @param {string} thumbnail_url - URL of the thumbnail
 * @param {string} thumbnail_name - Filename of the thumbnail
 * @returns {Promise<sqlite.RunResult>} - Result of the update operation
 */
export async function setThumbnailSaved(url, thumbnail_url, thumbnail_name) {
  try {
    if (!validateInput(url, 'string') || 
        !validateInput(thumbnail_url, 'string') || 
        !validateInput(thumbnail_name, 'string')) {
      throw new Error('Invalid parameters for thumbnail saved update');
    }
    
    dbLogger(LOG_LEVELS.DEBUG, 'Setting thumbnail as saved', { url, thumbnail_url });
    
    return await db.run(`
      UPDATE subdata
      SET
        is_thumbnail_saved = 1,
        thumbnail_url = ?,
        thumbnail_name = ?
      WHERE url = ?
    `, [thumbnail_url, thumbnail_name, url]);
  } catch (error) {
    dbLogger(LOG_LEVELS.ERROR, 'Error setting thumbnail as saved', { 
      url, 
      thumbnail_url,
      error: error.message,
      stack: error.stack
    });
    throw error;
  }
}

/**
 * Takes the given data for the given url and updates the appropriate database columns.
 * @param {String} url - URL of the submission
 * @param {Object} d - Object containing the data to update
 * @returns {Promise<sqlite.RunResult>} - Result of the update operation
 */
export async function saveMetaData(url, d) {
  try {
    if (!validateInput(url, 'string') || !validateInput(d, 'object')) {
      throw new Error('Invalid parameters for metadata update');
    }
    
    const data = [];
    let queryNames = Object.getOwnPropertyNames(d)
      .filter(key => {
        // Validate each property to prevent SQL injection
        const value = d[key];
        if (value === undefined || value === null) {
          return false;
        }
        // Ensure we're only updating valid columns to prevent injection attacks
        return typeof key === 'string' && key.match(/^[a-zA-Z0-9_]+$/);
      })
      .map(key => {
        data.push(d[key]);
        return `${key} = ?`;
      });
    
    if (queryNames.length === 0) {
      dbLogger(LOG_LEVELS.WARN, 'No valid data to update', { url });
      return null;
    }
    
    // Add the URL parameter for WHERE clause
    data.push(url);
    
    dbLogger(LOG_LEVELS.DEBUG, 'Updating metadata', { url, fields: Object.keys(d) });
    
    return await db.run(`
      UPDATE subdata
      SET 
        ${queryNames.join(',')}
      WHERE url = ?
    `, ...data);
  } catch (error) {
    dbLogger(LOG_LEVELS.ERROR, 'Error updating metadata', {
      url,
      error: error.message,
      stack: error.stack
    });
    throw error;
  }
}
/**
 * Creates blank entries in the database for all given submission URLs
 * for later updating.
 * @param {Array<Strings>} links 
 * @param {Boolean} isScraps 
 * @returns {Promise<null>}
 */
export function saveLinks(links, isScraps = false, username) {
  let placeholder = [];
  const data = links.reduce((acc, val) => {
    let data = [val, username, isScraps, false];
    let marks = `(${data.map(()=>'?').join(',')})`;
    acc.push(...data);
    placeholder.push(marks);
    return acc;
    }, []);
  return genericInsert('subdata', 'url, username, is_scrap, is_content_saved', placeholder, data);
}
export function saveComments(comments) {
  let placeholder = [];
  const data = comments.reduce((acc, c) => {
    let data = [
      c.id,
      c.submission_id,
      c.width,
      c.username,
      c.account_name,
      c.desc,
      c.subtitle,
      c.date,
    ];
    let marks = `(${data.map(()=>'?').join(',')})`;
    acc.push(...data);
    placeholder.push(marks);
    return acc;
  }, []);
  placeholder.join(',');
  return db.run(`
  INSERT INTO commentdata (
    id,
    submission_id,
    width,
    username,
    account_name,
    desc,
    subtitle,
    date
  ) 
  VALUES ${placeholder}
  ON CONFLICT(id) DO UPDATE SET
    desc = excluded.desc,
    date = excluded.date
  `, ...data);
}
/**
 * Saves user's favorites with transaction support
 * @param {String} username - Username of the user
 * @param {Array<String>} links - Array of submission URLs marked as favorites
 * @returns {Promise<sqlite.RunResult>} - Result of the insert operation
 */
export async function saveFavorites(username, links) {
  if (!validateInput(username, 'string') || !validateInput(links, 'array') || links.length === 0) {
    dbLogger(LOG_LEVELS.WARN, 'Invalid parameters for saving favorites', { username, linkCount: links?.length });
    return null;
  }
  
  dbLogger(LOG_LEVELS.INFO, 'Saving favorites', { username, linkCount: links.length });
  
  // Start a transaction for atomic operation
  try {
    await db.run('BEGIN TRANSACTION');
    
    let placeholder = [];
    const data = links.reduce((acc, val) => {
      if (!validateInput(val, 'string')) {
        return acc; // Skip invalid URLs
      }
      
      // Create a unique ID by combining username and URL
      let data = [`${username}_${val}`, username, val];
      let marks = `(${data.map(() => '?').join(',')})`;
      acc.push(...data);
      placeholder.push(marks);
      return acc;
    }, []);
    
    if (placeholder.length === 0) {
      dbLogger(LOG_LEVELS.WARN, 'No valid favorites to save', { username });
      await db.run('ROLLBACK');
      return null;
    }
    
    // Insert favorites
    await genericInsert('favorites', 'id, username, url', placeholder, data);
    
    // Also update the subdata table to mark these submissions as favorites
    for (const url of links) {
      if (validateInput(url, 'string')) {
        await db.run(`
          UPDATE subdata
          SET 
            is_favorite = 1,
            favorite_username = ?
          WHERE url = ?
        `, [username, url]);
      }
    }
    
    // Commit the transaction
    await db.run('COMMIT');
    dbLogger(LOG_LEVELS.INFO, 'Favorites saved successfully', { username, count: links.length });
    return { changes: links.length };
    
  } catch (error) {
    // Rollback in case of any error
    await db.run('ROLLBACK');
    dbLogger(LOG_LEVELS.ERROR, 'Error saving favorites', { 
      username, 
      error: error.message,
      stack: error.stack
    });
    throw error;
  }
}
/**
 * Set all user settings
 * @param {Object} userSettings 
 */
export async function saveUserSettings(userSettings) {
  let data = Object.getOwnPropertyNames(userSettings)
  .map((key) => {
    let val = userSettings[key];
    switch (typeof val) {
      case 'string':
        val = `'${val}'`;
        break;
      case 'object':
        val = `'${JSON.stringify(val)}'`;
        break;
    }
    return `${key} = ${val}`
  });
  db.run(`
  UPDATE usersettings
  SET ${data.join(',')}
  `);
}
export function setOwnedAccount(username) {
  if (!username) return;
  return genericInsert('ownedaccounts', 'username', ['(?)'], [username]);
}
/**
 * Deletes an owned account from the database
 * @param {string} username - Username to delete
 * @returns {Promise<sqlite.RunResult>} - Result of the delete operation
 */
export async function deleteOwnedAccount(username) {
  try {
    if (!validateInput(username, 'string')) {
      throw new Error('Invalid username for deletion');
    }
    
    dbLogger(LOG_LEVELS.INFO, 'Deleting owned account', { username });
    
    return await db.run(`
      DELETE FROM ownedaccounts
      WHERE username = ?
    `, [username]);
  } catch (error) {
    dbLogger(LOG_LEVELS.ERROR, 'Error deleting owned account', { 
      username, 
      error: error.message,
      stack: error.stack
    });
    throw error;
  }
}

// SELECT/GET functions
/**
 * Gets gallery page with pagination and search functionality
 * @param {number} offset - Starting index for pagination
 * @param {number} count - Number of items per page
 * @param {Object} query - Search parameters
 * @param {string} sortOrder - Sort direction (ASC or DESC)
 * @returns {Promise<Array>} - Gallery items
 */
export async function getGalleryPage(offset = 0, count = 25, query = {}, sortOrder = 'DESC') {
  try {
    // Validate input parameters
    if (!validateInput(offset, 'number') || !validateInput(count, 'number')) {
      throw new Error('Invalid pagination parameters');
    }
    
    if (!['ASC', 'DESC'].includes(sortOrder.toUpperCase())) {
      sortOrder = 'DESC'; // Default to DESC if invalid
    }
    
    let { username, searchTerm, galleryType } = query;
    
    // Build query with parameters instead of string interpolation
    const params = [];
    const conditions = ['id IS NOT NULL'];
    
    // Add search term condition
    if (searchTerm) {
      const searchPattern = `%${searchTerm.replace(/\s/gi, '%')}%`;
      conditions.push(`(
        title LIKE ? OR
        tags LIKE ? OR
        desc LIKE ? OR
        content_name LIKE ?
      )`);
      params.push(searchPattern, searchPattern, searchPattern, searchPattern);
    }
    
    // Add gallery type condition (favorites)
    if (galleryType && username) {
      conditions.push(`url IN (
        SELECT url
        FROM favorites f
        WHERE f.username LIKE ?
      )`);
      params.push(`%${username.replace(/_/g, '%')}%`);
    } else if (username) {
      // Add username condition
      conditions.push(`(
        username LIKE ? OR
        account_name LIKE ?
      )`);
      params.push(`%${username}%`, `%${username}%`);
    }
    
    // Add pagination parameters
    params.push(count, offset);
    
    dbLogger(LOG_LEVELS.DEBUG, 'Getting gallery page', { 
      offset, 
      count, 
      search: searchTerm,
      username,
      galleryType 
    });
    
    // Construct the final query
    const dbQuery = `
      SELECT 
        id, 
        title,
        username,
        account_name,
        content_name,
        content_url,
        date_uploaded,
        is_content_saved,
        thumbnail_name,
        is_thumbnail_saved,
        rating,
        is_favorite,
        favorite_username,
        content_owner
      FROM subdata
      WHERE ${conditions.join(' AND ')}
      ORDER BY content_name ${sortOrder}
      LIMIT ? OFFSET ?
    `;
    
    return await db.all(dbQuery, params);
  } catch (error) {
    dbLogger(LOG_LEVELS.ERROR, 'Error getting gallery page', { 
      offset, 
      count, 
      query,
      error: error.message,
      stack: error.stack 
    });
    throw error;
  }
}
/**
 * Gets all submission info for given id including comments
 * @param {String} id - Submission ID
 * @returns {Promise<Object>} - Submission and comments data
 */
export async function getSubmissionPage(id) {
  try {
    if (!validateInput(id, 'string') && !validateInput(id, 'number')) {
      throw new Error('Invalid submission ID');
    }
    
    dbLogger(LOG_LEVELS.DEBUG, 'Getting submission page', { id });
    
    // Use a transaction to ensure consistency between submission and comments
    await db.run('BEGIN TRANSACTION');
    
    const data = {};
    
    // Get submission data
    data.submission = await db.get(`
      SELECT *
      FROM subdata
      WHERE id = ?
    `, [id]);
    
    if (!data.submission) {
      dbLogger(LOG_LEVELS.WARN, 'Submission not found', { id });
      await db.run('ROLLBACK');
      return null;
    }
    
    // Get comments for the submission
    data.comments = await db.all(`
      SELECT *
      FROM commentdata
      WHERE submission_id = ?
    `, [id]);
    
    await db.run('COMMIT');
    return data;
    
  } catch (error) {
    await db.run('ROLLBACK');
    dbLogger(LOG_LEVELS.ERROR, 'Error getting submission page', { 
      id, 
      error: error.message,
      stack: error.stack 
    });
    throw error;
  }
}

export function getAllUnmovedContentData() {
  return db.all(`
  SELECT content_url, content_name, username, account_name
  FROM subdata
  WHERE is_content_saved = 1
    AND moved_content = 0
  `);
}
/**
 * Finds all data with content not yet downloaded.
 * @param {string} [name] - Optional filter for username or account name
 * @returns {Promise<Array>} - Array of content that needs to be downloaded
 */
export async function getAllUnsavedContent(name) {
  try {
    const params = [];
    let nameQuery;
    
    if (name && validateInput(name, 'string')) {
      nameQuery = `AND (username LIKE ? OR account_name LIKE ?)`;
      params.push(`%${name}%`, `%${name}%`);
    } else {
      nameQuery = `AND username IS NOT NULL`;
    }
    
    dbLogger(LOG_LEVELS.DEBUG, 'Getting all unsaved content', { name });
    
    return await db.all(`
      SELECT content_url, content_name, username, account_name, url
      FROM subdata
      WHERE is_content_saved = 0
      AND content_missing = 0
      AND content_url IS NOT NULL
      AND content_name NOT LIKE '%.'
      ${nameQuery}
      ORDER BY content_name DESC
    `, params);
  } catch (error) {
    dbLogger(LOG_LEVELS.ERROR, 'Error getting unsaved content', { 
      name, 
      error: error.message,
      stack: error.stack 
    });
    throw error;
  }
}
/**
 * Finds all thumbnails that haven't been downloaded yet
 * @returns {Promise<Array>} - Array of thumbnails that need to be downloaded
 */
export async function getAllUnsavedThumbnails() {
  try {
    dbLogger(LOG_LEVELS.DEBUG, 'Getting all unsaved thumbnails');
    
    return await db.all(`
      SELECT url, content_url, username, thumbnail_url, account_name, id
      FROM subdata
      WHERE is_thumbnail_saved = 0
      AND username IS NOT NULL
      AND thumbnail_missing = 0
      AND (
        content_url LIKE '%/stories/%'
        OR content_url LIKE '%/music/%'
        OR content_url LIKE '%/poetry/%'
      )
      ORDER BY content_name DESC
    `);
  } catch (error) {
    dbLogger(LOG_LEVELS.ERROR, 'Error getting unsaved thumbnails', { 
      error: error.message,
      stack: error.stack 
    });
    throw error;
  }
}
/**
 * Queries for all entries that need data repair
 * @param {string} [username] - Optional filter for username
 * @returns {Promise<Array>} - All entries in need of repair
 */
export async function needsRepair(username) {
  try {
    const params = [];
    let usernameQuery = '';
    
    if (username && validateInput(username, 'string')) {
      usernameQuery = `AND (username = ? OR account_name = ?)`;
      params.push(username, username);
    }
    
    dbLogger(LOG_LEVELS.DEBUG, 'Getting submissions needing repair', { username });
    
    return await db.all(`
      SELECT url
      FROM subdata
      WHERE id IS NOT NULL
      ${usernameQuery}
      AND (
        username IS NULL
        OR rating IS NULL
        OR category IS NULL
        OR date_uploaded LIKE '%ago%'
        OR id IN (
          SELECT submission_id
          FROM commentdata
          WHERE date LIKE '%ago%'
        )
      )
    `, params);
  } catch (error) {
    dbLogger(LOG_LEVELS.ERROR, 'Error getting submissions needing repair', { 
      username, 
      error: error.message,
      stack: error.stack 
    });
    throw error;
  }
}
/**
 * Gets all usernames that have favorites in the database
 * @returns {Promise<Array>} - Array of usernames
 */
export async function getAllFavUsernames() {
  try {
    dbLogger(LOG_LEVELS.DEBUG, 'Getting all favorite usernames');
    
    return await db.all(`
      SELECT DISTINCT username
      FROM favorites
      WHERE username IS NOT NULL
      ORDER BY username ASC
    `);
  } catch (error) {
    dbLogger(LOG_LEVELS.ERROR, 'Error getting favorite usernames', { 
      error: error.message,
      stack: error.stack 
    });
    throw error;
  }
}
export function getAllUsernames() {
  return db.all(`
    SELECT DISTINCT username, account_name
    FROM subdata
    WHERE username IS NOT NULL
    ORDER BY username ASC
  `);
}

/**
 * Retrieves all submission links with uncollected data.
 * @param {Boolean} isScraps 
 * @returns {Promise<Array>} All matching Database rows
 */
export function getSubmissionLinks() {
  return db.all(`
    SELECT url
    FROM subdata
    WHERE id IS null
    ORDER BY url DESC
  `);
}

/**
 * Gets all comments for a specific submission
 * @param {string|number} id - The submission ID
 * @returns {Promise<Array>} - Array of comments
 */
export async function getComments(id) {
  try {
    if (!validateInput(id, 'string') && !validateInput(id, 'number')) {
      throw new Error('Invalid submission ID for comment retrieval');
    }
    
    dbLogger(LOG_LEVELS.DEBUG, 'Getting comments for submission', { id });
    
    return await db.all(`
      SELECT *
      FROM commentdata
      WHERE submission_id = ?
      AND desc IS NOT NULL
    `, [id]);
  } catch (error) {
    dbLogger(LOG_LEVELS.ERROR, 'Error getting comments', { 
      id, 
      error: error.message,
      stack: error.stack 
    });
    throw error;
  }
}

export function getOwnedAccounts() {
  return db.all(`
    SELECT *
    FROM ownedaccounts
  `).then(results => results.map(a => a.username))
    .catch(() => []);
}

/**
 * Gets all submissions for a specific user
 * @param {string} username - Username to get submissions for
 * @returns {Promise<Array>} - Array of user's submissions
 */
export async function getAllSubmissionsForUser(username) {
  try {
    if (!validateInput(username, 'string')) {
      throw new Error('Invalid username for submission retrieval');
    }
    
    dbLogger(LOG_LEVELS.DEBUG, 'Getting all submissions for user', { username });
    
    return await db.all(`
      SELECT *
      FROM subdata
      WHERE (
        username = ? OR account_name = ?
      )
      AND is_content_saved = 1
      AND id IS NOT NULL
      ORDER BY content_name ASC
    `, [username, username]);
  } catch (error) {
    dbLogger(LOG_LEVELS.ERROR, 'Error getting submissions for user', { 
      username, 
      error: error.message,
      stack: error.stack 
    });
    throw error;
  }
}
/**
 * 
 * @returns {Promise<Array>} All submission data in the database
 */
export function getAllSubmissionData() {
  return db.all('SELECT url from subdata');
}
/**
 * 
 * @returns {Promise<Array>} All complete submission data in the database
 */
export function getAllCompleteSubmissionData() {
  return db.all('SELECT * from subdata WHERE id IS NOT NULL');
}

/**
 * Gets the owner (username or account_name) of a submission based on content_url or url
 * @param {String} url The submission URL or content URL
 * @returns {Promise<Object>} The submission owner data
 */
export function getSubmissionOwner(url) {
  return db.get(`
    SELECT username, account_name
    FROM subdata
    WHERE content_url = ? OR url = ?
  `, [url, url]);
}
export function getAllInvalidFiles() {
  return db.all(`
    SELECT id, content_name, content_url, username, account_name, url
    FROM subdata
    WHERE content_name LIKE '%.'
    AND is_content_saved = 1
  `);
}

export async function getUserSettings() {
  return db.get(`SELECT * FROM usersettings`);
}

export async function close() {
  return db.close().catch(() => console.log('Database already closed!'));
}
/**
 * Deletes all info related to the given amount from submission data and favorites.
 * 
 * @param {String} name 
 * @returns {Promise<null>}
 */
export async function deleteAccount(name) {
  await db.run(`
    DELETE FROM subdata
    WHERE account_name = ?
    OR username = ?
  `, [name, name]);
  return db.run(`
    DELETE FROM favorites
    WHERE username = ?
  `, [name]);
}
/**
 * Checks if appropriate columns exist and adds them if not
 */
async function ensureColumns() {
  try {
    // Get the list of columns in the subdata table
    const tableInfo = await db.all(`PRAGMA table_info(subdata)`);
    const existingColumns = tableInfo.map(col => col.name);
    
    // Define the columns we plan to add
    const columnsToAdd = [
      { name: 'is_favorite', definition: 'INTEGER DEFAULT 0' },
      { name: 'favorite_username', definition: 'TEXT' },
      { name: 'content_owner', definition: 'TEXT' }
    ];
    
    // Iterate over columns to ensure they exist
    for (const col of columnsToAdd) {
      if (!existingColumns.includes(col.name)) {
        console.log(`[Database] Adding column: ${col.name}`);
        await db.exec(`ALTER TABLE subdata ADD COLUMN ${col.name} ${col.definition}`);
      }
    }
  } catch (error) {
    console.error('[Database] Error ensuring columns:', error.message);
  }
}
/**
 * Initializes the database connection and ensures all required tables and columns exist
 * @returns {Promise<void>} - Promise that resolves when initialization is complete
 */
export async function init() {
  // Prevent multiple simultaneous initialization attempts
  if (isConnecting) {
    dbLogger(LOG_LEVELS.WARN, 'Database initialization already in progress');
    // Wait for the current initialization to complete
    let retries = 0;
    while (isConnecting && retries < 10) {
      await new Promise(resolve => setTimeout(resolve, 500));
      retries++;
    }
    
    if (db) return;
    if (connectionError) throw connectionError;
  }
  
  isConnecting = true;
  connectionError = null;
  
  try {
    dbLogger(LOG_LEVELS.INFO, 'Initializing database connection', { path: dbLocation });
    
    // Ensure the database file exists
    await fs.ensureFile(dbLocation);
    
    // Enable verbose mode for better debugging
    sqlite3.verbose();
    
    // Configure connection options with timeouts and pragmas
    const connectionOptions = {
      filename: dbLocation,
      driver: sqlite3.cached.Database,
      timeout: 10000, // 10 seconds timeout
    };
    
    // Open the database connection
    db = await open(connectionOptions);
    
    // Set connection status
    connectionStatus.isOpen = true;
    connectionStatus.lastConnectTime = Date.now();
    connectionStatus.reconnectAttempts = 0;
    
    // Set pragmas for better performance and security
    await db.exec(`
      PRAGMA journal_mode = WAL;
      PRAGMA synchronous = NORMAL;
      PRAGMA foreign_keys = ON;
      PRAGMA temp_store = MEMORY;
    `);
    
    // Check for existence of necessary tables
    const tableCheck = await db.get(`
      SELECT name 
      FROM sqlite_master 
      WHERE type='table' AND name='subdata'
    `);
    // Create table if it doesn't exist
    if (!tableCheck || !tableCheck.name) {
      dbLogger(LOG_LEVELS.INFO, 'Creating subdata table');
      
      await db.exec(`
        CREATE TABLE subdata (
          id TEXT PRIMARY KEY,
          title TEXT, 
          desc TEXT, 
          tags TEXT, 
          url TEXT UNIQUE ON CONFLICT IGNORE, 
          is_scrap INTEGER, 
          date_uploaded TEXT, 
          content_url TEXT, 
          content_name TEXT, 
          is_content_saved INTEGER DEFAULT 0,
          username TEXT,
          account_name TEXT,
          rating TEXT,
          category TEXT,
          thumbnail_url TEXT,
          thumbnail_name TEXT,
          is_thumbnail_saved INTEGER DEFAULT 0,
          thumbnail_missing INTEGER DEFAULT 0,
          content_missing INTEGER DEFAULT 0,
          moved_content INTEGER DEFAULT 0,
          is_favorite INTEGER DEFAULT 0,
          favorite_username TEXT,
          content_owner TEXT
        )
      `);
      
      // Set database version
      await db.exec(`PRAGMA user_version = 2`);
    }
    
    // Check for favorites table
    const favoritesCheck = await db.get(`
      SELECT name 
      FROM sqlite_master 
      WHERE type='table' AND name='favorites'
    `);
    
    // Create favorites table if needed
    if (!favoritesCheck || !favoritesCheck.name) {
      dbLogger(LOG_LEVELS.INFO, 'Creating favorites table');
      
      await db.exec(`
        CREATE TABLE favorites (
          id TEXT PRIMARY KEY,
          username TEXT,
          url TEXT,
          UNIQUE(username, url) ON CONFLICT REPLACE
        )
      `);
    }
    
    // Check for comments table
    const commentsCheck = await db.get(`
      SELECT name 
      FROM sqlite_master 
      WHERE type='table' AND name='commentdata'
    `);
    
    // Create comments table if needed
    if (!commentsCheck || !commentsCheck.name) {
      dbLogger(LOG_LEVELS.INFO, 'Creating comments table');
      
      await db.exec(`
        CREATE TABLE commentdata (
          id TEXT PRIMARY KEY,
          submission_id TEXT,
          username TEXT,
          account_name TEXT,
          width TEXT,
          desc TEXT,
          subtitle TEXT,
          date TEXT
        )
      `);
    }
    
    // Check for user settings table
    const settingsCheck = await db.get(`
      SELECT name 
      FROM sqlite_master 
      WHERE type='table' AND name='usersettings'
    `);
    
    // Create user settings table if needed
    if (!settingsCheck || !settingsCheck.name) {
      dbLogger(LOG_LEVELS.INFO, 'Creating user settings table');
      
      await db.exec(`
        CREATE TABLE usersettings (
          id INTEGER PRIMARY KEY CHECK (id = 1),
          scrapeInProgress INTEGER DEFAULT 0,
          historyCollapsed INTEGER DEFAULT 0
        )
      `);
      
      // Insert default row
      await db.run(`
        INSERT INTO usersettings (id) VALUES (1)
      `);
    }
    
    // Check for owned accounts table
    const ownedAccountsCheck = await db.get(`
      SELECT name 
      FROM sqlite_master 
      WHERE type='table' AND name='ownedaccounts'
    `);
    
    // Create owned accounts table if needed
    if (!ownedAccountsCheck || !ownedAccountsCheck.name) {
      dbLogger(LOG_LEVELS.INFO, 'Creating owned accounts table');
      
      await db.exec(`
        CREATE TABLE ownedaccounts (
          username TEXT PRIMARY KEY
        )
      `);
    }
    
    // Run database upgrade process
    await upgradeDatabase(db);
    
    // Ensure all required columns exist
    await ensureColumns();
    
    dbLogger(LOG_LEVELS.INFO, 'Database initialization complete');
    isConnecting = false;
    return db;
    
  } catch (error) {
    connectionError = error;
    isConnecting = false;
    dbLogger(LOG_LEVELS.ERROR, 'Database initialization failed', {
      error: error.message,
      stack: error.stack
    });
    
    if (db) {
      try {
        await db.close();
      } catch (closeError) {
        dbLogger(LOG_LEVELS.ERROR, 'Error closing database', { 
          error: closeError.message 
        });
      }
    }
    
    throw error;
  }
}

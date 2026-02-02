#!/usr/bin/env node

require('dotenv').config();

const AWS = require('aws-sdk');
const https = require('https');
const { spawn } = require('child_process');
const fs = require('fs');
const path = require('path');

// Configuration
const API_VIDEO_KEY = process.env.API_VIDEO_KEY;
const DO_SPACES_KEY = process.env.DO_SPACES_KEY;
const DO_SPACES_SECRET = process.env.DO_SPACES_SECRET;
const DO_SPACES_REGION = process.env.DO_SPACES_REGION;
const DO_SPACES_BUCKET = process.env.DO_SPACES_BUCKET;

// Validate required environment variables
const requiredVars = ['API_VIDEO_KEY', 'DO_SPACES_KEY', 'DO_SPACES_SECRET', 'DO_SPACES_REGION', 'DO_SPACES_BUCKET'];
const missingVars = requiredVars.filter(varName => !process.env[varName]);

if (missingVars.length > 0) {
  console.error('Error: Missing required environment variables:');
  missingVars.forEach(varName => console.error(`  - ${varName}`));
  console.error('\nPlease create a .env file based on .env.example');
  process.exit(1);
}

const s3 = new AWS.S3({
  endpoint: `https://${DO_SPACES_REGION}.digitaloceanspaces.com`,
  accessKeyId: DO_SPACES_KEY,
  secretAccessKey: DO_SPACES_SECRET,
  region: DO_SPACES_REGION,
  s3ForcePathStyle: false,
});

// Retry configuration
const MAX_RETRIES = 3;
const RETRY_DELAY = 5000; // 5 seconds between retries

// Failed videos log file
const FAILED_LOG_FILE = path.join(__dirname, 'failed-videos.json');

// Fetch all videos with pagination
async function fetchAllVideos() {
  const allVideos = [];
  let currentPage = 1;
  let hasMorePages = true;
  
  console.log('Fetching all videos from api.video...\n');
  
  while (hasMorePages) {
    console.log(`  Fetching page ${currentPage}...`);
    
    const pageData = await fetchVideoPage(currentPage);
    
    if (pageData && pageData.length > 0) {
      allVideos.push(...pageData);
      currentPage++;
      // Continue if we got a full page (assume page size is 100)
      hasMorePages = pageData.length === 100;
    } else {
      hasMorePages = false;
    }
  }
  
  console.log(`  Total videos found: ${allVideos.length}\n`);
  return allVideos;
}

// Fetch single page of videos
async function fetchVideoPage(page) {
  return new Promise((resolve, reject) => {
    const curl = spawn('curl', [
      '-s',
      '-H', `Authorization: Bearer ${API_VIDEO_KEY}`,
      `https://ws.api.video/videos?currentPage=${page}&pageSize=100`
    ]);
    
    let data = '';
    curl.stdout.on('data', (chunk) => {
      data += chunk;
    });
    
    curl.on('close', (code) => {
      if (code !== 0) {
        reject(new Error(`curl exited with code ${code}`));
        return;
      }
      try {
        const response = JSON.parse(data);
        resolve(response.data);
      } catch (err) {
        reject(new Error(`Failed to parse JSON: ${err.message}`));
      }
    });
    
    curl.on('error', reject);
  });
}

// Fetch single video details using curl
async function fetchVideoDetails(videoId) {
  return new Promise((resolve, reject) => {
    const curl = spawn('curl', [
      '-s',
      '-H', `Authorization: Bearer ${API_VIDEO_KEY}`,
      `https://ws.api.video/videos/${videoId}`
    ]);
    
    let data = '';
    curl.stdout.on('data', (chunk) => {
      data += chunk;
    });
    
    curl.on('close', (code) => {
      if (code !== 0) {
        reject(new Error(`curl exited with code ${code}`));
        return;
      }
      try {
        const response = JSON.parse(data);
        resolve(response);
      } catch (err) {
        reject(new Error(`Failed to parse JSON: ${err.message}`));
      }
    });
    
    curl.on('error', reject);
  });
}

// Stream video directly from URL to S3 with retry logic
async function streamToS3WithRetry(sourceUrl, s3Key, videoTitle, retries = 0) {
  try {
    await streamToS3(sourceUrl, s3Key, videoTitle);
    return { success: true };
  } catch (error) {
    if (retries < MAX_RETRIES) {
      console.log(`  🔄 Retrying (${retries + 1}/${MAX_RETRIES})...`);
      await delay(RETRY_DELAY);
      return streamToS3WithRetry(sourceUrl, s3Key, videoTitle, retries + 1);
    } else {
      return { success: false, error: error.message };
    }
  }
}

function delay(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

// Stream video directly from URL to S3 (no temp storage)
async function streamToS3(sourceUrl, s3Key, videoTitle) {
  return new Promise((resolve, reject) => {
    console.log(`  📤 Starting stream to S3...`);
    
    https.get(sourceUrl, {
      headers: {
        'User-Agent': 'Mozilla/5.0 (Node.js Transfer Script)'
      }
    }, (response) => {
      if (response.statusCode !== 200) {
        reject(new Error(`Failed to download: ${response.statusCode}`));
        return;
      }
      
      const contentLength = response.headers['content-length'];
      const sizeMB = contentLength ? (parseInt(contentLength) / 1024 / 1024).toFixed(2) : 'unknown';
      console.log(`  📦 Size: ${sizeMB} MB`);
      
      // Stream directly to S3 using multipart upload
      const uploadParams = {
        Bucket: DO_SPACES_BUCKET,
        Key: s3Key,
        Body: response,
        ContentType: 'video/mp4',
        ACL: 'private',
        StorageClass: 'STANDARD',
      };
      
      if (contentLength) {
        uploadParams.ContentLength = parseInt(contentLength);
      }
      
      s3.upload(uploadParams, {
        partSize: 10 * 1024 * 1024, // 10MB chunks
        queueSize: 1 // Process one chunk at a time (low memory)
      }, (err, data) => {
        if (err) {
          reject(err);
        } else {
          console.log(`  ✅ Uploaded to: ${data.Location || s3Key}`);
          resolve(data);
        }
      });
      
    }).on('error', reject);
  });
}

// Load failed videos from previous run
function loadFailedVideos() {
  if (fs.existsSync(FAILED_LOG_FILE)) {
    try {
      const data = fs.readFileSync(FAILED_LOG_FILE, 'utf8');
      return JSON.parse(data);
    } catch (err) {
      console.log('Warning: Could not load failed videos log, starting fresh');
      return [];
    }
  }
  return [];
}

// Save failed videos for retry later
function saveFailedVideos(failedVideos) {
  fs.writeFileSync(FAILED_LOG_FILE, JSON.stringify(failedVideos, null, 2));
  console.log(`\n📄 Saved ${failedVideos.length} failed videos to ${FAILED_LOG_FILE}`);
  console.log('   Run with --retry flag to retry failed transfers\n');
}

// Clear failed videos log
function clearFailedLog() {
  if (fs.existsSync(FAILED_LOG_FILE)) {
    fs.unlinkSync(FAILED_LOG_FILE);
    console.log('Cleared failed videos log\n');
  }
}

// Check if file already exists in S3
async function checkFileExists(s3Key) {
  return new Promise((resolve) => {
    s3.headObject({
      Bucket: DO_SPACES_BUCKET,
      Key: s3Key
    }, (err) => {
      if (err && err.code === 'NotFound') {
        resolve(false);
      } else if (err) {
        // Some other error, assume doesn't exist to be safe
        resolve(false);
      } else {
        resolve(true);
      }
    });
  });
}

async function transferVideos(videosToProcess) {
  const failedVideos = [];
  
  console.log(`Processing ${videosToProcess.length} videos...\n`);

  for (let i = 0; i < videosToProcess.length; i++) {
    const video = videosToProcess[i];
    console.log(`[${i + 1}/${videosToProcess.length}] Processing: ${video.title || video.videoId}`);
    
    try {
      // Get video details with source URL
      const videoDetails = await fetchVideoDetails(video.videoId);
      const sourceUrl = videoDetails.assets?.mp4;
      
      if (!sourceUrl) {
        console.log(`  ⚠️ No MP4 source available for video ${video.videoId}\n`);
        continue;
      }

      // Sanitize title for use as filename
      const sanitizeFilename = (title) => {
        return title
          .replace(/[^a-zA-Z0-9\-\s.]/g, '') // Remove special chars except dash, space, dot
          .replace(/\s+/g, ' ') // Collapse multiple spaces
          .trim() || video.videoId; // Fallback to videoId if empty
      };
      
      const safeTitle = sanitizeFilename(video.title || video.videoId);
      const s3Key = `api-video-backup/${safeTitle} - ${video.videoId}`;

      // Check if file already exists in S3
      const exists = await checkFileExists(s3Key);
      if (exists) {
        console.log(`  ⏭️  Already exists in S3, skipping\n`);
        continue;
      }

      // Stream directly from URL to S3 with retry
      const result = await streamToS3WithRetry(sourceUrl, s3Key, video.title);
      
      if (result.success) {
        console.log(`  ✅ Successfully transferred\n`);
      } else {
        console.error(`  ❌ Failed after ${MAX_RETRIES} retries: ${result.error}`);
        failedVideos.push({
          videoId: video.videoId,
          title: video.title,
          error: result.error,
          timestamp: new Date().toISOString()
        });
        console.log('');
      }
    } catch (error) {
      console.error(`  ❌ Failed to process video ${video.videoId}:`, error.message);
      failedVideos.push({
        videoId: video.videoId,
        title: video.title,
        error: error.message,
        timestamp: new Date().toISOString()
      });
      console.log('');
    }
  }

  return failedVideos;
}

// List all objects in S3 bucket with given prefix
async function listS3Objects(prefix) {
  return new Promise((resolve, reject) => {
    const objects = [];
    const listObjects = (marker) => {
      const params = {
        Bucket: DO_SPACES_BUCKET,
        Prefix: prefix,
        MaxKeys: 1000
      };
      if (marker) params.Marker = marker;
      
      s3.listObjects(params, (err, data) => {
        if (err) {
          reject(err);
          return;
        }
        
        objects.push(...data.Contents);
        
        if (data.IsTruncated) {
          // More objects to fetch
          listObjects(data.Contents[data.Contents.length - 1].Key);
        } else {
          resolve(objects);
        }
      });
    };
    
    listObjects();
  });
}

// Verify transfer - check which videos are missing from S3
async function verifyTransfer() {
  console.log('🔍 Verifying transfer...\n');
  
  // Get all videos from api.video
  const allVideos = await fetchAllVideos();
  console.log(`Found ${allVideos.length} videos in api.video\n`);
  
  // Get all files from S3
  console.log('Listing files in S3...');
  const s3Objects = await listS3Objects('api-video-backup/');
  console.log(`Found ${s3Objects.length} files in S3\n`);
  
  // Create a map of S3 keys
  const s3Keys = new Set(s3Objects.map(obj => obj.Key));
  
  // Check each video
  const missingVideos = [];
  const sizeMismatch = [];
  
  for (const video of allVideos) {
    const sanitizeFilename = (title) => {
      return title
        .replace(/[^a-zA-Z0-9\-\s.]/g, '')
        .replace(/\s+/g, ' ')
        .trim() || video.videoId;
    };
    
    const safeTitle = sanitizeFilename(video.title || video.videoId);
    const expectedKey = `api-video-backup/${safeTitle} - ${video.videoId}`;
    
    if (!s3Keys.has(expectedKey)) {
      missingVideos.push({
        videoId: video.videoId,
        title: video.title,
        expectedKey
      });
    }
  }
  
  // Report results
  console.log('\n📊 Verification Results:');
  console.log('========================');
  console.log(`Total videos in api.video: ${allVideos.length}`);
  console.log(`Total files in S3: ${s3Objects.length}`);
  console.log(`Missing from S3: ${missingVideos.length}`);
  
  if (missingVideos.length > 0) {
    console.log('\n❌ Missing Videos:');
    missingVideos.forEach((v, i) => {
      console.log(`  ${i + 1}. ${v.title || 'Untitled'} (${v.videoId})`);
    });
    
    // Save missing videos for retry
    const missingForRetry = missingVideos.map(v => ({
      videoId: v.videoId,
      title: v.title,
      error: 'Missing from S3 after verification',
      timestamp: new Date().toISOString()
    }));
    
    fs.writeFileSync(FAILED_LOG_FILE, JSON.stringify(missingForRetry, null, 2));
    console.log(`\n📄 Saved ${missingVideos.length} missing videos to ${FAILED_LOG_FILE}`);
    console.log('   Run with --retry flag to transfer missing videos\n');
  } else {
    console.log('\n✅ All videos verified! Nothing missing from S3.');
    clearFailedLog();
  }
  
  return missingVideos;
}

async function main() {
  const args = process.argv.slice(2);
  const retryMode = args.includes('--retry');
  const retryFailed = args.includes('--retry-failed');
  const verifyMode = args.includes('--verify');
  
  try {
    if (verifyMode) {
      await verifyTransfer();
      return;
    }
    
    if (retryMode || retryFailed) {
      // Retry only failed videos from previous run
      const failedVideos = loadFailedVideos();
      
      if (failedVideos.length === 0) {
        console.log('No failed videos to retry.\n');
        return;
      }
      
      console.log(`🔄 Retrying ${failedVideos.length} failed videos...\n`);
      
      // Convert back to the format expected by transferVideos
      const videosToRetry = failedVideos.map(v => ({
        videoId: v.videoId,
        title: v.title
      }));
      
      const stillFailed = await transferVideos(videosToRetry);
      
      if (stillFailed.length === 0) {
        console.log('\n🎉 All videos successfully transferred!');
        clearFailedLog();
      } else {
        console.log(`\n⚠️ ${stillFailed.length} videos still failed`);
        saveFailedVideos(stillFailed);
      }
      
    } else {
      // Normal mode: process all videos
      const videos = await fetchAllVideos();
      
      const failedVideos = await transferVideos(videos);
      
      if (failedVideos.length === 0) {
        console.log('\n🎉 Transfer complete! All videos transferred successfully!');
        clearFailedLog();
      } else {
        console.log(`\n⚠️ Transfer complete with ${failedVideos.length} failures`);
        saveFailedVideos(failedVideos);
        console.log('\nTo retry failed videos, run: npm run transfer -- --retry');
      }
    }
    
  } catch (error) {
    console.error('Error:', error.message);
    process.exit(1);
  }
}

// Run the transfer
main();

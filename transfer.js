#!/usr/bin/env node

require('dotenv').config();

const AWS = require('aws-sdk');
const https = require('https');
const { spawn } = require('child_process');

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

// Fetch videos list using curl
async function fetchVideoList() {
  return new Promise((resolve, reject) => {
    const curl = spawn('curl', [
      '-s',
      '-H', `Authorization: Bearer ${API_VIDEO_KEY}`,
      'https://ws.api.video/videos?currentPage=1&pageSize=100'
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

async function transferVideos() {
  try {
    console.log('Fetching videos from api.video...\n');
    
    const videos = await fetchVideoList();
    
    console.log(`Found ${videos.length} videos to transfer\n`);

    for (let i = 0; i < videos.length; i++) {
      const video = videos[i];
      console.log(`[${i + 1}/${videos.length}] Processing: ${video.title || video.videoId}`);
      
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

        // Stream directly from URL to S3 (no temp storage!)
        await streamToS3(sourceUrl, s3Key, video.title);

        console.log(`  ✅ Successfully transferred\n`);
      } catch (error) {
        console.error(`  ❌ Failed to transfer video ${video.videoId}:`, error.message);
        console.log('');
      }
    }

    console.log('\nTransfer complete!');
    
  } catch (error) {
    console.error('Error:', error.message);
    process.exit(1);
  }
}

// Run the transfer
transferVideos();

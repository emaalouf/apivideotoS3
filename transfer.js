#!/usr/bin/env node

require('dotenv').config();

const { ApiVideoClient } = require('@api.video/nodejs-client');
const AWS = require('aws-sdk');
const https = require('https');
const fs = require('fs');
const path = require('path');
const { pipeline } = require('stream/promises');

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

// Initialize clients
const apiVideoClient = new ApiVideoClient({ apiKey: API_VIDEO_KEY });

const s3 = new AWS.S3({
  endpoint: `https://${DO_SPACES_REGION}.digitaloceanspaces.com`,
  accessKeyId: DO_SPACES_KEY,
  secretAccessKey: DO_SPACES_SECRET,
  region: DO_SPACES_REGION,
  s3ForcePathStyle: false,
});

// Create temp directory
const TEMP_DIR = path.join(__dirname, 'temp');
if (!fs.existsSync(TEMP_DIR)) {
  fs.mkdirSync(TEMP_DIR, { recursive: true });
}

async function downloadFile(url, filePath) {
  return new Promise((resolve, reject) => {
    const file = fs.createWriteStream(filePath);
    https.get(url, (response) => {
      if (response.statusCode !== 200) {
        reject(new Error(`Failed to download: ${response.statusCode}`));
        return;
      }
      response.pipe(file);
      file.on('finish', () => {
        file.close();
        resolve();
      });
    }).on('error', reject);
  });
}

async function uploadToSpaces(filePath, key) {
  const fileContent = fs.readFileSync(filePath);
  
  const params = {
    Bucket: DO_SPACES_BUCKET,
    Key: key,
    Body: fileContent,
    ACL: 'private',
    StorageClass: 'STANDARD', // Change to 'GLACIER' for cold storage
  };

  await s3.upload(params).promise();
}

async function transferVideos() {
  try {
    console.log('Fetching videos from api.video...');
    
    const videosResponse = await apiVideoClient.videos.list({});
    const videos = videosResponse.data;
    
    console.log(`Found ${videos.length} videos to transfer\n`);

    for (let i = 0; i < videos.length; i++) {
      const video = videos[i];
      console.log(`[${i + 1}/${videos.length}] Processing: ${video.title || video.videoId}`);
      
      try {
        // Get video source URL
        const videoResponse = await apiVideoClient.videos.get(video.videoId);
        const sourceUrl = videoResponse.assets?.mp4;
        
        if (!sourceUrl) {
          console.log(`  ⚠️ No MP4 source available for video ${video.videoId}`);
          continue;
        }

        const tempFile = path.join(TEMP_DIR, `${video.videoId}.mp4`);
        const s3Key = `api-video-backup/${video.videoId}.mp4`;

        // Download video
        console.log(`  📥 Downloading...`);
        await downloadFile(sourceUrl, tempFile);
        const stats = fs.statSync(tempFile);
        console.log(`  📦 Size: ${(stats.size / 1024 / 1024).toFixed(2)} MB`);

        // Upload to Digital Ocean
        console.log(`  ☁️ Uploading to Digital Ocean Spaces...`);
        await uploadToSpaces(tempFile, s3Key);

        // Clean up temp file
        fs.unlinkSync(tempFile);

        console.log(`  ✅ Successfully transferred\n`);
      } catch (error) {
        console.error(`  ❌ Failed to transfer video ${video.videoId}:`, error.message);
      }
    }

    console.log('\nTransfer complete!');
    
    // Clean up temp directory
    fs.rmSync(TEMP_DIR, { recursive: true, force: true });
    
  } catch (error) {
    console.error('Error:', error.message);
    process.exit(1);
  }
}

// Run the transfer
transferVideos();

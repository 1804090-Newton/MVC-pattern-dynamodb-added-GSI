const { DynamoDBClient } = require('@aws-sdk/client-dynamodb');
const dotenv = require('dotenv');

dotenv.config();

const client = new DynamoDBClient({
  region: process.env.AWS_REGION,
  endpoint: process.env.AWS_ENDPOINT,
  apiVersion: process.env.AWS_API_VERSION
});

module.exports = { client };





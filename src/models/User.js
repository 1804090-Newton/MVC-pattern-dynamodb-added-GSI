const {  PutItemCommand, GetItemCommand, UpdateItemCommand, DeleteItemCommand, ScanCommand,QueryCommand } = require("@aws-sdk/client-dynamodb");
const { marshall, unmarshall } = require("@aws-sdk/util-dynamodb");
const { client } = require('../config/aws');

const TABLE_NAME = 'Users';
const GSI_NAME = 'EmailIndex';

const User = {
  async getAll() {
    const params = {
      TableName: TABLE_NAME
    };

    const command = new ScanCommand(params);

    return client.send(command)
      .then(data => data.Items.map(item => unmarshall(item)))
      .catch(error => {
        console.error("Unable to scan the table. Error:", error);
        throw error;
      });
  },
      


  async getById(id) {
    const params = {
      TableName: TABLE_NAME,
      Key: marshall({ id: parseInt(id) })
    };

    const command = new GetItemCommand(params);

    return client.send(command)
      .then(data => unmarshall(data.Item))
      .catch(error => {
        console.error("Unable to read item. Error:", error);
        throw error;
      });
  },

  async createUser(userData) {
    const params = {
      TableName: TABLE_NAME,
      Item: marshall({ id: Date.now(), ...userData })
    };

    const command = new PutItemCommand(params);

    return client.send(command)
      .then(() => unmarshall(params.Item))
      .catch(error => {
        console.error("Unable to add item. Error:", error);
        throw error;
      });
  },

  
  async getByEmail(email) {
    try {
      const params = {
        TableName: TABLE_NAME,
        IndexName: GSI_NAME, 
        KeyConditionExpression: "#email = :email",
        ExpressionAttributeNames: {
          "#email": "email",
        },
        ExpressionAttributeValues: marshall({ ":email": email }),
        ProjectionExpression: "id, email, username", 
        ProvisionedThroughput: {
          ReadCapacityUnits: 5,
          WriteCapacityUnits: 5 
        }
      };
  
      const data = await client.send(new QueryCommand(params));
      return unmarshall(data.Items[0]);
    } catch (error) {
      console.error("Unable to read item. Error:", error);
      throw error; 
    }
  },
  

  async updateUser(id, userData) {
    const updateExpression = buildUpdateExpression(userData);
    const params = buildUpdateParams(id, userData, updateExpression);

    const command = new UpdateItemCommand(params);

    return client.send(command)
      .then(data => unmarshall(data.Attributes))
      .catch(error => {
        console.error("Unable to update item. Error:", error);
        throw error;
      });
  },

  async patchUser(id, userData) {
    try {
      const existingItem = await this.getById(id);
      
      if (existingItem) {
        const updatedUserData = {
          username: userData.username || existingItem.username,
          email: userData.email || existingItem.email,
          password: userData.password || existingItem.password
        };
        
        await this.updateUser(id, updatedUserData);
        return updatedUserData;
      } else {
        throw new Error("Item not found");
      }
    } catch (error) {
      console.error("Unable to patch item. Error:", error);
      throw error;
    }
  },

  async deleteUser(id) {
    const params = {
      TableName: TABLE_NAME,
      Key: marshall({ id: parseInt(id) }),
      ReturnValues: "ALL_OLD"
    };

    const command = new DeleteItemCommand(params);

    return client.send(command)
      .then(data => unmarshall(data.Attributes))
      .catch(error => {
        console.error("Unable to delete item. Error:", error);
        throw error;
      });
  }
};

function buildUpdateExpression(userData) {
  return "SET " + Object.keys(userData).map(key => `#${key} = :${key}`).join(", ");
}

function buildUpdateParams(id, userData, updateExpression) {
  const expressionAttributeNames = {};
  const expressionAttributeValues = {};

  for (const key in userData) {
    expressionAttributeNames[`#${key}`] = key;
    expressionAttributeValues[`:${key}`] = userData[key];
  }

  return {
    TableName: TABLE_NAME,
    Key: marshall({ id: parseInt(id) }),
    UpdateExpression: updateExpression,
    ExpressionAttributeNames: expressionAttributeNames,
    ExpressionAttributeValues: marshall(expressionAttributeValues),
    ReturnValues: "ALL_NEW"
  };
}

module.exports = User;


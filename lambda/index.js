const AWSXRay = require('aws-xray-sdk-core');
const AWS = AWSXRay.captureAWS(require('aws-sdk'));

async function publishSNSMessage(message, event_type, issue_status, flow_sid) {

    var params = {
        Message: JSON.stringify(message),
        /* required */
        TopicArn: process.env.SNS_TOPIC,
        MessageAttributes: {
            "event.type": {
                DataType: "String",
                StringValue: event_type
            },
            "status": {
                DataType: "String",
                StringValue: issue_status
            },
            "event.flow": {
                DataType: "String",
                StringValue: flow_sid
            }
        }
    };

    // Create promise and SNS service object
    try {
        var publishTextPromise = new AWS
            .SNS({apiVersion: '2010-03-31'})
            .publish(params)
            .promise();

        await publishTextPromise.then(function (data) {
            console.log(`Message ${params.Message} send sent to the topic ${params.TopicArn}`);
            console.log("Message " + data);
            var jsonData = JSON.parse(params.Message);

            console.log("json data:");
            console.log(jsonData);
            return;
        })
            .catch(function (err) {
                console.error(err, err.stack);
                return err;
            });

    } catch (err) {
        return err;
    }

}

exports.handler = async(event) => {

    AWS
        .config
        .update({accessKeyId: process.env.AWS_KEY, secretAccessKey: process.env.AWS_SECRET});

    AWS
        .config
        .update({region: "us-west-1"});

    console.log("INCOMING RECORD.MESSAGE: ");
    console.log(event.Records[0]);
    
    //let's parse this incoming record
    let message = JSON.parse(event.Records[0].body);
    let event_type = event.Records[0].messageAttributes["event.type"].stringValue;
    let flow_sid = event.Records[0].messageAttributes["event.flow"].stringValue;

    console.log("INCOMING RECORD.MESSAGE: ");
    console.log("EVENT TYPE: " + event_type);
    //need to parse twice since the json gets escaped twice

    console.log("message: ");
    console.log(message);

    let entity = message.entity;
    let entityKey = message.key;
    //if the timestamp doesn't exist, then this is a new record and we should add it
    entityKey.timestamp = (!entityKey.timestamp)
        ? parseInt(event.Records[0].attributes.SentTimestamp)
        : parseInt(entityKey.timestamp);

    let params = message.params;

    let update_expression = "SET ";
    let expression_attribute_names = {};
    let expression_attribute_values = {};

    let current_element = 0;

    Object
        .keys(message.params)
        .forEach((key) => {
            current_element++;
            let comma = (current_element === Object.keys(params).length)
                ? ""
                : ", ";
            update_expression = update_expression.concat("#" + key + " = :" + key + comma);
            expression_attribute_names["#" + key] = key;
            expression_attribute_values[":" + key] = params[key];
        });
    console.log(update_expression);
    console.log(expression_attribute_names);
    console.log(expression_attribute_values);

    //look for empty or null inputs
    if (entity === null || entity === "") {
        //callback("Sent empty entity;", null);
    } else {
        entity = entity.trim();
    }

    var docClient = new AWS
        .DynamoDB
        .DocumentClient({convertEmptyValues: true, endpoint: "dynamodb.us-west-1.amazonaws.com"});

    console.log(entity);
    console.log(entityKey);

    var db_params = {
        TableName: entity,
        Key: entityKey,
        "UpdateExpression": update_expression,
        "ExpressionAttributeNames": expression_attribute_names,
        "ExpressionAttributeValues": expression_attribute_values,
        "ReturnValues": "ALL_NEW"
    };

    console.log('executing updateObjectPromise');

    await docClient
        .update(db_params)
        .promise()
        .then(async(item) => {
            console.log("item: ");
            console.log(item);
            //here we want to broadcast an sns message indicating what we just did:
            let response_event_type = "";
            switch (event_type) {
                case "open.new.request":
                    response_event_type = "new.request.opened";
                    break;
                case "update.request.status":
                    response_event_type = "request.status.updated";
                    break;
            }

            await publishSNSMessage({
                "from": message.from,
                "to": message.to,
                "params": {
                    "type": response_event_type,
                    "request": item.Attributes
                }
            }, response_event_type, item.Attributes.status, flow_sid).then(() => {
                console.log(entity + " update");
                console.log(item);
                return item;
            });
        })
        .catch((error) => {
            console.log("ERROR: ");
            console.log(error);
            return error;
        });

    console.log("Here's an item.");
    // }); const response = {     statusCode: 200,     body: JSON.stringify(bod + "
    // and hello from me!") }; return response;
};

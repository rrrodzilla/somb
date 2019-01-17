const AWS = require('aws-sdk');
AWS
    .config
    .update({accessKeyId: process.env.AWS_KEY, secretAccessKey: process.env.AWS_SECRET});

AWS
    .config
    .update({region: "us-west-1"});

async function publishSNSMessage(message, event_type, flow_sid) {

    var params = {
        Message: JSON.stringify(message),
        /* required */
        TopicArn: process.env.SNS_TOPIC,
        MessageAttributes: {
            "event.type": {
                DataType: "String",
                StringValue: event_type
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

async function asyncForEach(array, callback) {
    for (let index = 0; index < array.length; index++) {
        await callback(array[index], index, array);
    }
}


exports.handler = async(event) => {

    

    //let's parse this incoming record
    let message = JSON.parse(event.Records[0].body);
    let event_type = event.Records[0].messageAttributes["event.type"].stringValue;
    let flow_sid = event.Records[0].messageAttributes["event.flow"].stringValue;

    console.log("INCOMING RECORD.MESSAGE: ");
    console.log("EVENT TYPE: " + event_type);
    //need to parse twice since the json gets escaped twice

    console.log("message: ");
    console.log(message);

    //we want to check the database for an existing open issue for this user

    var docClient = new AWS
        .DynamoDB
        .DocumentClient({endpoint: "dynamodb.us-west-1.amazonaws.com"});

    var params = {
        TableName: "need_request",
        IndexName: "phone_number-status-index",
        KeyConditionExpression: "phone_number = :phone_number AND #request_status = :status ",
        ExpressionAttributeValues: {
            ":phone_number": message.to,
            ":status": "open"
        },
        ExpressionAttributeNames: {
            "#request_status": "status"
        }
    };

    let queryPromise = docClient
        .query(params)
        .promise();

    await queryPromise.then(async(results) => {
        //then send a message based on whether something was found or not
        await publishSNSMessage({
            "from": message.from,
            "to": message.to,
            "params": (results.Items.length === 0)
                ? {
                    "type": "open.issue.not.found"
                }
                : {
                    "type": "open.issue.found",
                    "request": results.Items[0]
                }
        }, (results.Items.length === 0)
            ? "open.issue.not.found"
            : "open.issue.found", flow_sid).then((data) => {
            return;
        });

    })
};

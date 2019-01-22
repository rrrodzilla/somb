const AWS = require('aws-sdk');

async function asyncForEach(array, callback) {
    for (let index = 0; index < array.length; index++) {
        await callback(array[index], index, array);
    }
}

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

exports.handler = async(event) => {

    AWS
        .config
        .update({accessKeyId: process.env.AWS_KEY, secretAccessKey: process.env.AWS_SECRET});

    AWS
        .config
        .update({region: "us-west-1"});

    await asyncForEach(event.Records, async(record) => {
        console.log("INCOMING RECORD.MESSAGE: ");
        console.log(record);

        //let's parse this incoming record
        let message = JSON.parse(record.body);
        let event_type = record.messageAttributes["event.type"].stringValue;
        let flow_sid = record.messageAttributes["event.flow"].stringValue;

        console.log("INCOMING RECORD.MESSAGE: ");
        console.log("EVENT TYPE: " + event_type);
        //need to parse twice since the json gets escaped twice

        console.log("message: ");
        console.log(message);

        let entity = message.entity;
        let entityKey = message.key;
        //if the timestamp doesn't exist, then this is a new record and we should add it
        entityKey.timestamp = (!entityKey.timestamp)
            ? parseInt(record.attributes.SentTimestamp)
            : parseInt(entityKey.timestamp);

        var counter_params = {
            "TableName": "counters",
            "ReturnValues": "UPDATED_NEW",
            "ExpressionAttributeValues": {
                ":a": 1
            },
            "ExpressionAttributeNames": {
                "#v": "current_value"
            },
            "UpdateExpression": "SET #v = #v + :a",
            "Key": {
                "counter_name": "need_requests"
            }

        };

        var docClient = new AWS
            .DynamoDB
            .DocumentClient({convertEmptyValues: true, endpoint: "dynamodb.us-west-1.amazonaws.com"});

        let fetchIdPromise = docClient
            .update(counter_params)
            .promise();

        console.log(entity);
        console.log(entityKey);

        console.log('executing fetchIdPromise');

        await fetchIdPromise.then(async(item) => {
            console.log("item: ");
            console.log(item);
            let fetched_id = item.Attributes.current_value;

            message.params.id = fetched_id;

            await publishSNSMessage(message, "upsert.new.request", flow_sid).then(() => {
                console.log(entity + " upsert");
                console.log(item);
                return item;
            });
        }).catch((error) => {
            console.log("ERROR: ");
            console.log(error);
            return error;
        });
    });

};

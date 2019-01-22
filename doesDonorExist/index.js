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

    await asyncForEach(event.Records, async(record) => {
        //let's parse this incoming record
        let message = JSON.parse(record.body);
        let event_type = record.messageAttributes["event.type"].stringValue;
        let flow_sid = record.messageAttributes["event.flow"].stringValue;

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
            TableName: "volunteers",
            IndexName: "rangeKey-index",
            KeyConditionExpression: "rangeKey = :phone_number",
            ExpressionAttributeValues: {
                ":phone_number": message.to
            }
        };

        let queryPromise = docClient
            .query(params)
            .promise();

        await queryPromise.then(async(results) => {
            //here we need to determine if this donor exists
            let donor_exists = (results.Items.length > 0);

            let post_event_type = (donor_exists)
                ? "donor.exists"
                : "donor.does.not.exist";

            let geo = (donor_exists)
                ? JSON.parse(results.Items[0].geoJson)
                : null;

            //if not, we can simply forward the donor.exists message
            let msg_params = {
                "from": message.from,
                "to": message.to,
                "params": (donor_exists)
                    ? {
                        "type": post_event_type,
                        "request": results.Items[0],
                        "lat": geo.coordinates[1],
                        "lon": geo.coordinates[0]
                    }
                    : {
                        "type": post_event_type
                    }
            };

            // if this donor does exist we need to inspect the message to see if any
            // commands were sent
            let potential_commands = message
                .body
                .toLowerCase()
                .split(" ");

            if (potential_commands.length > 1) {
                console.log(potential_commands[0]);
                switch (potential_commands[0]) {
                    case "give" || "deliver" || "both":
                        if (potential_commands.length > 1) {
                            //ok we found a command, let's decorate our message before sending it out
                            let need_request_id = potential_commands[1];
                            post_event_type = "donor.request.command";
                            msg_params.params = {
                                "type": post_event_type,
                                "command": potential_commands[0],
                                "need_request_id": need_request_id,
                                "donor": results.Items[0]
                            };
                            break;
                        } else 
                            break;
                        default:
                        break;
                }
            }

            await publishSNSMessage(msg_params, post_event_type, flow_sid).then((data) => {
                return;
            });

        });

    });

};

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

async function broadcastMsg(from, to, requestId, eventType, flow_sid) {
    let msg_params = {
        "from": from,
        "to": to,
        "params": {
            "type": eventType,
            "request_id": requestId
        }
    };

    await publishSNSMessage(msg_params, eventType, flow_sid).then(() => {
        return;
    });

}

async function updateStatusMsg(from, to, request_obj, status, flow_sid) {
    let msg_params = {
        "from": from,
        "to": to,
        "entity": "need_request",
        "key": {
            "phone_number": request_obj.phone_number,
            "timestamp": request_obj.timestamp
        },
        "params": {
            "fulfillment_status": status,
            "donor": (request_obj.donor)
                ? request_obj.donor
                : {},
            "deliverer": (request_obj.deliverer)
                ? request_obj.deliverer
                : {}
        }
    };

    await publishSNSMessage(msg_params, "update.request.status.to." + status, flow_sid).then(() => {
        return;
    });

}

async function asyncForEach(array, callback) {
    for (let index = 0; index < array.length; index++) {
        await callback(array[index], index, array);
    }
}

const CANCELED_REQUEST_RESPONSE = "donor.command.response.canceled.request";
const EXPIRED_REQUEST_RESPONSE = "donor.command.response.expired.request";
const DONOR_EXISTS_RESPONSE = "donor.command.response.donor.already.exists";
const AWAITING_DELIVERY_RESPONSE = "donor.command.response.already.awaiting.delivery";
const ALREADY_FULFILLED_RESPONSE = "donor.command.response.already.fulfilled";
const DELIVERER_EXISTS_RESPONSE = "donor.command.response.deliverer.already.exists";
const INVALID_REQUEST_RESPONSE = "donor.command.response.invalid.request.id";

exports.handler = async(event) => {

    var docClient = new AWS
        .DynamoDB
        .DocumentClient({endpoint: "dynamodb.us-west-1.amazonaws.com"});

    await asyncForEach(event.Records, async(record) => {
        //let's parse this incoming record
        let {message, flow_sid} = parseRecord(record);

        // in this function we want to load the need_request so we can check out the
        // status the status is going to determine how we handle this incoming command
        // we want to check the database for an existing open issue for this user

        var params = {
            TableName: "need_request",
            IndexName: "id-index-lookup",
            KeyConditionExpression: "id = :lookup_id",
            ExpressionAttributeValues: {
                ":lookup_id": parseInt(message.params.request_id, 10)
            }
        };

        let queryPromise = docClient
            .query(params)
            .promise();

        const STATUS_CANCELED = "cancelled";
        const STATUS_EXPIRED = "expired";
        const STATUS_OPEN = "open";
        const STATUS_DONOR_FOUND = "donor.found";
        const STATUS_DELIVERER_FOUND = "deliverer.found";
        const STATUS_AWAITING_DELIVERY = "awaiting.delivery";
        const STATUS_FULFILLED = "fulfilled";

        await queryPromise.then(async(results) => {
            //now we can determine if this record even exists
            let request_exists = (results.Items.length > 0);

            if (!request_exists) {
                //we should send a message to let the donor know this record doesn't exist
                await broadcastMsg(message.from, message.to, message.params.request_id, INVALID_REQUEST_RESPONSE, flow_sid).then(() => {
                    return;
                });
            } else {
                let need_request = results.Items[0];
                console.log(need_request);
                var response_message = "";

                switch (need_request.status) {
                    case STATUS_CANCELED:
                        response_message = CANCELED_REQUEST_RESPONSE;
                        break;
                    case STATUS_EXPIRED:
                        response_message = EXPIRED_REQUEST_RESPONSE;
                        break;
                    case STATUS_OPEN:
                        switch (message.params.command) {
                            case "give":
                                switch (need_request.fulfillment_status) {
                                    case STATUS_OPEN:
                                        need_request.donor = message.to;
                                        await updateStatusMsg(message.from, message.to, need_request, STATUS_DONOR_FOUND, flow_sid).then(() => {
                                            return;
                                        });
                                        //updated_status = STATUS_DONOR_FOUND; //and add donor to request record
                                        break;
                                    case STATUS_DONOR_FOUND:
                                        response_message = DONOR_EXISTS_RESPONSE;
                                        break;
                                    case STATUS_DELIVERER_FOUND:
                                        need_request.donor = message.to;
                                        await updateStatusMsg(message.from, message.to, need_request, STATUS_AWAITING_DELIVERY, flow_sid).then(() => {
                                            return;
                                        });
                                        //updated_status = STATUS_AWAITING_DELIVERY; //and add donor to request record
                                        break;
                                    case STATUS_AWAITING_DELIVERY:
                                        response_message = AWAITING_DELIVERY_RESPONSE;
                                        break;
                                    case STATUS_FULFILLED:
                                        response_message = ALREADY_FULFILLED_RESPONSE;
                                        break;
                                }
                                break;
                            case "deliver":
                                switch (need_request.fulfillment_status) {
                                    case STATUS_OPEN:
                                        need_request.deliverer = message.to;
                                        await updateStatusMsg(message.from, message.to, need_request, STATUS_DELIVERER_FOUND, flow_sid).then(() => {
                                            return;
                                        });
                                        // updated_status = STATUS_DELIVERER_FOUND; //and add donor to request record
                                        break;
                                    case STATUS_DONOR_FOUND:
                                        need_request.deliverer = message.to;
                                        await updateStatusMsg(message.from, message.to, need_request, STATUS_AWAITING_DELIVERY, flow_sid).then(() => {
                                            return;
                                        });
                                        // updated_status = STATUS_AWAITING_DELIVERY; //and add donor to request record
                                        break;
                                    case STATUS_DELIVERER_FOUND:
                                        response_message = DELIVERER_EXISTS_RESPONSE;
                                        break;
                                    case STATUS_AWAITING_DELIVERY:
                                        response_message = AWAITING_DELIVERY_RESPONSE;
                                        break;
                                    case STATUS_FULFILLED:
                                        response_message = ALREADY_FULFILLED_RESPONSE;
                                        break;
                                }
                                break;
                            case "provide":
                                switch (need_request.fulfillment_status) {
                                    case STATUS_OPEN:
                                        need_request.deliverer = message.to;
                                        need_request.donor = message.to;
                                        await updateStatusMsg(message.from, message.to, need_request, STATUS_AWAITING_DELIVERY, flow_sid).then(() => {
                                            return;
                                        });
                                        // updated_status = STATUS_AWAITING_DELIVERY; //and add donor to request record
                                        break;
                                    case STATUS_DONOR_FOUND:
                                        response_message = DONOR_EXISTS_RESPONSE;
                                        break;
                                    case STATUS_DELIVERER_FOUND:
                                        response_message = DELIVERER_EXISTS_RESPONSE;
                                        break;
                                    case STATUS_AWAITING_DELIVERY:
                                        response_message = AWAITING_DELIVERY_RESPONSE;
                                        break;
                                    case STATUS_FULFILLED:
                                        response_message = ALREADY_FULFILLED_RESPONSE;
                                        break;
                                }
                                break;

                        }
                }

                //let's see what command we have and then send he appropriate status update

                if (response_message !== "") {
                    await broadcastMsg(message.from, message.to, message.params.request_id, response_message, flow_sid).then(() => {
                        return;
                    });

                }

            }

        });

    });

};

function parseRecord(record) {
    let message = JSON.parse(record.body);
    let event_type = record.messageAttributes["event.type"].stringValue;
    let flow_sid = record.messageAttributes["event.flow"].stringValue;

    console.log("INCOMING RECORD.MESSAGE: ");
    console.log("EVENT TYPE: " + event_type);
    //need to parse twice since the json gets escaped twice

    console.log("message: ");
    console.log(message);

    return {message, flow_sid};
}

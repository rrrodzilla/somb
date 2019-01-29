const client = require('twilio')(process.env.ACCOUNTSID, process.env.AUTHTOKEN);

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

    console.log("trying setting up proxy service");
    try {

        let proxySetup = client
            .proxy
            .services('KS7a5580757bb77a274abce0f4a3bcfb7c')
            .sessions
            .create({uniqueName: message.params.request.id});
        await proxySetup.then(async(session) => {
            console.log(session.sid)
            let donorSetup = client
                .proxy
                .services('KS7a5580757bb77a274abce0f4a3bcfb7c')
                .sessions(session.sid)
                .participants
                .create({friendlyName: "donor", identifier: message.params.request.donor});
            await donorSetup.then(async(donor_participant) => {
                console.log("donor_participant.proxyIdentifier: " + donor_participant.proxyIdentifier);
                let delivererSetup = client
                    .proxy
                    .services('KS7a5580757bb77a274abce0f4a3bcfb7c')
                    .sessions(session.sid)
                    .participants
                    .create({friendlyName: "deliverer", identifier: "+12062022603"}); //+12062022603
                //                    .create({friendlyName: "deliverer", identifier: message.params.request.deliverer}); //+12062022603
                await delivererSetup.then((deliverer_participant) => {
                    console.log("deliverer_participant.proxyIdentifier: " + deliverer_participant.proxyIdentifier);
                    return;
                });
            });
        });

    } catch (err) {
        console.log("ERROR:" + err);
        return err;
    }
};

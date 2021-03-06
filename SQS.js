/*** dependencies ***/
var _ = require ( 'lodash' );
/********************/

/*** logging ********/
var loggerConfig = {
    logLevels: {
        SQS: 'ERROR'
    }
};

var l = require ( 'tm-log' ), logger = l.logger ( __filename, loggerConfig ), inspect = l.inspect;
/********************/

var sqsFactory = function ( sqs ) {
    return function ( sqsUrl ) {
        logger.trace ( 'Got sqsUrl' );
        logger.debug ( sqsUrl );

        return function ( sqsCommand ) {
            logger.trace ( 'Got sqsCommand' );
            logger.debug ( sqsCommand );

            if ( sqsCommand === 'buildMessage' ) {
                return function ( buffer, callBack ) {
                    callBack ( null, {
                        MessageBody: buffer.toString ( 'base64' ),
                        QueueUrl: sqsUrl,
                        DelaySeconds: 0,
                    } );
                };
            }

            if ( sqsCommand === 'breakMessage' ) {
                return function ( message, callBack ) {
                    var output;

                    if ( _.isUndefined ( message.Body ) ) { 
                        callBack ( 'invalid message - no Body' );
                        return;
                    }

                    callback ( null, new Buffer ( message.Body, 'base64' ) );
                };
            }

            return function ( parms ) {
                logger.trace ( 'Got parms' );
                logger.debug ( parms );

                return function ( callBack ) {
                    var fullParms = _.extend ( parms, { QueueUrl: sqsUrl } );

                    logger.trace ( 'Got callBack' );
                    logger.trace ( 'full parms:' );
                    logger.debug ( fullParms );

                    sqs[sqsCommand] ( fullParms, function ( error, data ) {
                        if ( error ) {
                            logger.error ( error );
                            callBack ( error );
                            return;
                        }

                        logger.trace ( 'Got data' );
                        logger.debug ( data );
                        callBack ( null, data );
                    } );
                };
            };
        };
    };
};

exports.sqs = function ( config ) {
    var AWS = require ( 'aws-sdk' );

    AWS.config.update ( _.pick ( config.aws, [ 'accessKeyId', 'secretAccessKey', 'region', 'paramValidation', 'computeChecksums' ] ) );

    return sqsFactory ( new AWS.SQS );
};

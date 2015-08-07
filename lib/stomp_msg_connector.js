var Stomp = require('stompjs2');
var smb = require('stomp_msg_broker');
var StompMsgBroker = smb.StompMsgBroker;
var MsgTransactionManager = smb.MsgTransactionManager;
var uuid = require('node-uuid');

var serverContext = {
	config: null
	,brokers: {}		// broker by broker name
	,txStarts: {}	// transaction start function by transaction name
};

var idGenerator = {
	request: function (onIdGenerated) {
		if (typeof onIdGenerated === 'function') onIdGenerated(uuid.v4());
	}
};

// progress object constructor
function progress () {
	var handlers = {};
	this.on = function(event, handler) {
		handlers[event] = handler;
		return this;
	};
	this.fireEvent = function(event, arg) {
		if (typeof handlers[event] === 'function') handlers[event](arg);
	};
}

function getStompClientCreator(broker_url, protocols, tlsOptions) {
	return (function () {
		return Stomp.client(broker_url, protocols, tlsOptions);
	});
}

function getBrokerOnConnectHandler(broker_name, broker_url, p) {
	return (function () {
		p.num_brokers_connected++;
		p.fireEvent('broker_connected', {broker_name: broker_name, broker_url: broker_url});
		if (p.num_brokers_connected == p.num_brokers)
			p.fireEvent('ready', {});
	});
}
function getBrokerOnDisconnectHandler(broker_name, reconnectIntervalMS) {
	return (function () {
		console.log(broker_name + ': ' + '!!! Disconnectted remotely !!! Will reconnect in ' + reconnectIntervalMS + ' ms');
	});
}
function getBrokerOnErrorHandler(broker_name) {
	return (function (e) {
		console.error(broker_name + ': ' + '!!! ERROR !!! ' + JSON.stringify(e));
	});
}
function getBrokerOnHeartbeatHandler(broker_name) {
	return (function (source) {
		console.log(broker_name + ': ' + source + " (-)HEART_BEAT(-)");
	});
}
function getBrokerOnDebugHandler(broker_name) {
	return (function (msg) {
		console.log(broker_name + ': ' + msg);
	});
}
function getProcessorOnMessageHandler(broker, process_handler_paths) {
	return (function (message) {
		var destination = message.headers['destination'];
		var handler_def = process_handler_paths[destination];
		if (handler_def && handler_def.require_path) {
			try {
				var handler = require(handler_def.require_path);
				if (handler && handler_def.handler_key) handler = handler[handler_def.handler_key];
				if (typeof handler === 'function') handler(broker, message);
			}
			catch (e) { console.error(e); }
		}
	});
}
function getTransactionOnMessageHandler(trans_incoming) {
	return (function (message) {
		var destination = message.headers['destination'];
		if (trans_incoming[destination]) {
			var handler = MsgTransactionManager.getBorkerOnMessageHandler();
			handler(message);
		}
	});
}
function getOnMessageHandler(onmessageHandlers) {
	return (function (message) {
		for (var i in onmessageHandlers) {	// for each onmessage handler
			var handler = onmessageHandlers[i];
			handler(message);
		}
	});
}

// initialize the message brokers
// supported events
// 1. broker_connected
// 2. ready
// 3. error
function initialize(config) {
	var p = new progress();
	p.num_brokers = 0;
	p.num_brokers_connected = 0;
	if (!config) throw 'bad config';
	serverContext.config = config;
	var brokers_config = config.brokers;
	if (!brokers_config) throw 'bad broker config';
	for (var broker_name in brokers_config) {	// for each broker
		p.num_brokers++;
		var broker_config = brokers_config[broker_name];
		var broker_url = broker_config.url;

		var destinations = {};
		var des = [];
		var processors = [];
		var transactions = [];
		var process_handler_paths = {};

		if (broker_config.processors) {
			for (var processor in broker_config.processors) {	// for each processor
				var processor_def = broker_config.processors[processor];
				var destination = processor_def.incoming;
				destinations[destination] = { headers: processor_def.subscribe_headers };
				destinations[destination].autoAckByClient = (typeof processor_def.autoAckByClient != 'boolean' ? true : processor_def.autoAckByClient);
				des.push(destination);
				processors.push(processor);
				process_handler_paths[destination] = {require_path: processor_def.handler_path};
				if (processor_def.handler_key) process_handler_paths[destination].handler_key = processor_def.handler_key;
			}
		}
		if (broker_config.transactions) {
			for (var transaction in broker_config.transactions) {	// for each transaction
				var transaction_def = broker_config.transactions[transaction];
				var destination = transaction_def.incoming;
				destinations[destination] = { headers: transaction_def.subscribe_headers };
				destinations[destination].autoAckByClient = (typeof transaction_def.autoAckByClient != 'boolean' ? true : transaction_def.autoAckByClient);
				des.push(destination);
				transactions.push(transaction);
			}
		}
		if (des.length > 0) console.log(broker_name + ': ' + 'listening to destinations: ' + JSON.stringify(des));
		// create the broker
		var protocols = null;
		var tlsOptions = (broker_config.tlsOptions ? broker_config.tlsOptions : null);
		var broker = new StompMsgBroker(getStompClientCreator(broker_url, protocols, tlsOptions), broker_config.broker_options, broker_config.login_options, destinations);
		serverContext.brokers[broker_name] = broker;

		var trans_incoming = {};
		for (var i in transactions) {	// for each transaction
			var transaction = transactions[i];
			var transaction_def = broker_config.transactions[transaction];
			serverContext.txStarts[transaction] = MsgTransactionManager.getTxStart(broker, idGenerator, transaction_def.outgoing, transaction_def.incoming);
			trans_incoming[transaction_def.incoming] = true;
		}

		broker.onconnect = getBrokerOnConnectHandler(broker_name, broker_url, p);
		broker.ondisconnect = getBrokerOnDisconnectHandler(broker_name, broker_config.broker_options.reconnectIntervalMS);
		broker.onerror = getBrokerOnErrorHandler(broker_name);
		if (broker_config.show_heart_beat) broker.onheartbeat = getBrokerOnHeartbeatHandler(broker_name);
		if (broker_config.debug) broker.debug = getBrokerOnDebugHandler(broker_name);
		var onmessageHandlers = [];
		if (processors.length > 0) onmessageHandlers.push(getProcessorOnMessageHandler(broker, process_handler_paths));
		if (transactions.length > 0) onmessageHandlers.push(getTransactionOnMessageHandler(trans_incoming));
		broker.onmessage = getOnMessageHandler(onmessageHandlers);
	}
	return p;
}

module.exports.initialize = initialize;
module.exports.listenerContext = serverContext;
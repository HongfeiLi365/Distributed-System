/**********************************
 * FILE NAME: MP2Node.cpp
 *
 * DESCRIPTION: MP2Node class definition
 **********************************/
#include "MP2Node.h"

/**
 * constructor
 */
MP2Node::MP2Node(Member *memberNode, Params *par, EmulNet * emulNet, Log * log, Address * address) {
	this->memberNode = memberNode;
	this->par = par;
	this->emulNet = emulNet;
	this->log = log;
	ht = new HashTable();
	this->memberNode->addr = *address;
}

/**
 * Destructor
 */
MP2Node::~MP2Node() {
	delete ht;
	delete memberNode;
}

/**
 * FUNCTION NAME: updateRing
 *
 * DESCRIPTION: This function does the following:
 * 				1) Gets the current membership list from the Membership Protocol (MP1Node)
 * 				   The membership list is returned as a vector of Nodes. See Node class in Node.h
 * 				2) Constructs the ring based on the membership list
 * 				3) Calls the Stabilization Protocol
 */
void MP2Node::updateRing() {
	/*
	 * Implement this. Parts of it are already implemented
	 */
	vector<Node> curMemList;
	bool change = false;

	/*
	 *  Step 1. Get the current membership list from Membership Protocol / MP1
	 */
	curMemList = getMembershipList();

	/*
	 * Step 2: Construct the ring
	 */
	// Sort the list based on the hashCode
	sort(curMemList.begin(), curMemList.end());
	ring = curMemList;
	
	/*
	 * Step 3: Run the stabilization protocol IF REQUIRED
	 */
	// Run stabilization protocol if the hash table size is greater than zero and if there has been a changed in the ring
	stabilizationProtocol();
	
}

/**
 * FUNCTION NAME: getMemberhipList
 *
 * DESCRIPTION: This function goes through the membership list from the Membership protocol/MP1 and
 * 				i) generates the hash code for each member
 * 				ii) populates the ring member in MP2Node class
 * 				It returns a vector of Nodes. Each element in the vector contain the following fields:
 * 				a) Address of the node
 * 				b) Hash code obtained by consistent hashing of the Address
 */
vector<Node> MP2Node::getMembershipList() {
	unsigned int i;
	vector<Node> curMemList;
	for ( i = 0 ; i < this->memberNode->memberList.size(); i++ ) {
		Address addressOfThisMember;
		int id = this->memberNode->memberList.at(i).getid();
		short port = this->memberNode->memberList.at(i).getport();
		memcpy(&addressOfThisMember.addr[0], &id, sizeof(int));
		memcpy(&addressOfThisMember.addr[4], &port, sizeof(short));
		curMemList.emplace_back(Node(addressOfThisMember));
	}
	return curMemList;
}

/**
 * FUNCTION NAME: hashFunction
 *
 * DESCRIPTION: This functions hashes the key and returns the position on the ring
 * 				HASH FUNCTION USED FOR CONSISTENT HASHING
 *
 * RETURNS:
 * size_t position on the ring
 */
size_t MP2Node::hashFunction(string key) {
	std::hash<string> hashFunc;
	size_t ret = hashFunc(key);
	return ret%RING_SIZE;
}








/**
 * FUNCTION NAME: clientCreate
 *
 * DESCRIPTION: client side CREATE API
 * 				The function does the following:
 * 				1) Constructs the message
 * 				2) Finds the replicas of this key
 * 				3) Sends a message to the replica
 */
void MP2Node::clientCreate(string key, string value) {
	/*
	 * Implement this
	 */
	dispatchMessages(CREATE, key, value);

	
}

/**
 * FUNCTION NAME: clientRead
 *
 * DESCRIPTION: client side READ API
 * 				The function does the following:
 * 				1) Constructs the message
 * 				2) Finds the replicas of this key
 * 				3) Sends a message to the replica
 */
void MP2Node::clientRead(string key){
	/*
	 * Implement this
	 */
	dispatchMessages(READ, key, "");
}

/**
 * FUNCTION NAME: clientUpdate
 *
 * DESCRIPTION: client side UPDATE API
 * 				The function does the following:
 * 				1) Constructs the message
 * 				2) Finds the replicas of this key
 * 				3) Sends a message to the replica
 */
void MP2Node::clientUpdate(string key, string value){
	/*
	 * Implement this
	 */
	dispatchMessages(UPDATE, key, value);
}

/**
 * FUNCTION NAME: clientDelete
 *
 * DESCRIPTION: client side DELETE API
 * 				The function does the following:
 * 				1) Constructs the message
 * 				2) Finds the replicas of this key
 * 				3) Sends a message to the replica
 */
void MP2Node::clientDelete(string key){
	/*
	 * Implement this
	 */
	dispatchMessages(DELETE, key, "");
}



void MP2Node::dispatchMessages(MessageType type, string key, string value) {
	vector<Node> receivers = findNodes(key);
	
	record *rec = (record *) malloc(sizeof(record));
	rec->id = g_transID++;
	rec->total_response=0;
	rec->success_response=0;
	rec->type=type;
	rec->key = new string(key);
	rec->value = new string(value);
	rec->cur_time = par->getcurrtime();
	database[rec->id]=rec;
	
	Message *message;
	
	message = new Message(rec->id, memberNode->addr, type, key, value, PRIMARY);
	emulNet->ENsend(&memberNode->addr, &receivers.at(0).nodeAddress, message->toString());
	delete message;

	message = new Message(rec->id, memberNode->addr, type, key, value, SECONDARY);
	emulNet->ENsend(&memberNode->addr, &receivers.at(1).nodeAddress, message->toString());
	delete message;	
	
	message = new Message(rec->id, memberNode->addr, type, key, value, TERTIARY);
	emulNet->ENsend(&memberNode->addr, &receivers.at(2).nodeAddress, message->toString());
	delete message;	
}






/**
 * FUNCTION NAME: createKeyValue
 *
 * DESCRIPTION: Server side CREATE API
 * 			   	The function does the following:
 * 			   	1) Inserts key value into the local hash table
 * 			   	2) Return true or false based on success or failure
 */
bool MP2Node::createKeyValue(string key, string value, ReplicaType replica, int trans_id) {
	/*
	 * Implement this
	 */
	// Insert key, value, replicaType into the hash table
	if (ht->create(key, value)) {
		// log success
		log->logCreateSuccess(&memberNode->addr, false, trans_id, key, value);
		return true;
	} else {
		// log failure
		log->logCreateFail(&memberNode->addr, false, trans_id, key, value);
		return false;
	}
}

/**
 * FUNCTION NAME: readKey
 *
 * DESCRIPTION: Server side READ API
 * 			    This function does the following:
 * 			    1) Read key from local hash table
 * 			    2) Return value
 */
string MP2Node::readKey(string key, int trans_id) {
	/*
	 * Implement this
	 */
	// Read key from local hash table and return value
	string value = ht->read(key);

	if (value != "") {
		// log success
		log->logReadSuccess(&memberNode->addr, false, trans_id, key, value);
	} else {
		// log failure
		log->logReadFail(&memberNode->addr, false, trans_id, key);
	}
	return value;
}

/**
 * FUNCTION NAME: updateKeyValue
 *
 * DESCRIPTION: Server side UPDATE API
 * 				This function does the following:
 * 				1) Update the key to the new value in the local hash table
 * 				2) Return true or false based on success or failure
 */
bool MP2Node::updateKeyValue(string key, string value, ReplicaType replica, int trans_id) {
	/*
	 * Implement this
	 */
	// Update key in local hash table and return true or false
	if (ht->update(key, value)) {
		// log success
		log->logUpdateSuccess(&memberNode->addr, false, trans_id, key, value);
		return true;
	} else {
		// log failure
		log->logUpdateFail(&memberNode->addr, false, trans_id, key, value);
		return false;
	}
}

/**
 * FUNCTION NAME: deleteKey
 *
 * DESCRIPTION: Server side DELETE API
 * 				This function does the following:
 * 				1) Delete the key from the local hash table
 * 				2) Return true or false based on success or failure
 */
bool MP2Node::deletekey(string key, int trans_id) {
	/*
	 * Implement this
	 */
	// Delete the key from the local hash table
	if (ht->deleteKey(key)) {
		// log success
		log->logDeleteSuccess(&memberNode->addr, false, trans_id, key);
		return true;
	} else {
		// log failure
		log->logDeleteFail(&memberNode->addr, false, trans_id, key);
		return false;
	}
}

/**
 * FUNCTION NAME: checkMessages
 *
 * DESCRIPTION: This function is the message handler of this node.
 * 				This function does the following:
 * 				1) Pops messages from the queue
 * 				2) Handles the messages according to message types
 */
void MP2Node::checkMessages() {
	/*
	 * Implement this. Parts of it are already implemented
	 */
	char * data;
	int size;

	/*
	 * Declare your local variables here
	 */

	// dequeue all messages and handle them
	while ( !memberNode->mp2q.empty() ) {
		/*
		 * Pop a message from the queue
		 */
		data = (char *)memberNode->mp2q.front().elt;
		size = memberNode->mp2q.front().size;
		memberNode->mp2q.pop();

		string message(data, data + size);

		/*
		 * Handle the message types here
		 */
		 
		Message *msgRcvd = new Message(message);
		Message *msgSend;
		bool status;
		record *rec;
		
		switch(msgRcvd->type) {
			case CREATE:
				status = createKeyValue(msgRcvd->key, msgRcvd->value, msgRcvd->replica, msgRcvd->transID);
				msgSend = new Message(msgRcvd->transID, memberNode->addr, REPLY, status);
				emulNet->ENsend(&memberNode->addr, &msgRcvd->fromAddr, msgSend->toString());
				delete msgSend;
				break;
			case READ:
				msgSend = new Message(msgRcvd->transID, memberNode->addr, READREPLY, "");
				msgSend->value = readKey(msgRcvd->key, msgRcvd->transID);
				emulNet->ENsend(&memberNode->addr, &msgRcvd->fromAddr, msgSend->toString());
				delete msgSend;
				break;
			case UPDATE:
				status = updateKeyValue(msgRcvd->key, msgRcvd->value, msgRcvd->replica, msgRcvd->transID);
				msgSend = new Message(msgRcvd->transID, memberNode->addr, REPLY, status);
				emulNet->ENsend(&memberNode->addr, &msgRcvd->fromAddr, msgSend->toString());
				delete msgSend;
				break;
			case DELETE:
				status = deletekey(msgRcvd->key, msgRcvd->transID);
				msgSend = new Message(msgRcvd->transID, memberNode->addr, REPLY, status);
				emulNet->ENsend(&memberNode->addr, &msgRcvd->fromAddr, msgSend->toString());
				delete msgSend;
				break;
			case REPLY:
				if (database.find(msgRcvd->transID) != database.end()) {
					rec=database[msgRcvd->transID];
					rec->total_response++;
					if (msgRcvd->success) { 
						rec->success_response++;
					}
					checkQuorum(rec);
				} 
				break;
			case READREPLY:
				if (database.find(msgRcvd->transID) != database.end()) {
					rec=database[msgRcvd->transID];
					rec->total_response++;
					if (msgRcvd->value != "") {
						rec->success_response++;
						rec->value=new string(msgRcvd->value);
					}
					checkQuorum(rec);
				} 
				break;
		}
		delete msgRcvd;
	}

	/*
	 * This function should also ensure all READ and UPDATE operation
	 * get QUORUM replies
	 */
	record *rec;
	for(auto const& it : database) {
		rec = database[it.first];
		if ((par->getcurrtime() - rec->cur_time) > 3) {
			rec->total_response++;
			checkQuorum(rec);
		}
	}
}




void MP2Node::checkQuorum(record *rec){
	if (rec->success_response == 2) {
		switch(rec->type) {
			case CREATE:
				log->logCreateSuccess(&memberNode->addr, true, rec->id, *(rec->key), *(rec->value));
				break;
			case READ:
				log->logReadSuccess(&memberNode->addr, true, rec->id, *(rec->key), *(rec->value));
				break;
			case UPDATE:
				log->logUpdateSuccess(&memberNode->addr, true, rec->id, *(rec->key), *(rec->value));
				break;
			case DELETE:
				log->logDeleteSuccess(&memberNode->addr, true, rec->id, *(rec->key));
				break;
			default:
				break;
		}
		database.erase(rec->id);
		free(rec);
	} else if (rec->total_response == 3 && rec->success_response < 2) {
		switch(rec->type) {
			case CREATE:
				log->logCreateFail(&memberNode->addr, true, rec->id, *(rec->key), *(rec->value));
				break;
			case READ:
				log->logReadFail(&memberNode->addr, true, rec->id, *(rec->key));
				break;
			case UPDATE:
				log->logUpdateFail(&memberNode->addr, true, rec->id, *(rec->key), *(rec->value));
				break;
			case DELETE:
				log->logDeleteFail(&memberNode->addr, true, rec->id, *(rec->key));
				break;
			default:
				break;
		}
		database.erase(rec->id);
		free(rec);
	}
}

/**
 * FUNCTION NAME: findNodes
 *
 * DESCRIPTION: Find the replicas of the given keyfunction
 * 				This function is responsible for finding the replicas of a key
 */
vector<Node> MP2Node::findNodes(string key) {
	size_t pos = hashFunction(key);
	vector<Node> addr_vec;
	if (ring.size() >= 3) {
		// if pos <= min || pos > max, the leader is the min
		if (pos <= ring.at(0).getHashCode() || pos > ring.at(ring.size()-1).getHashCode()) {
			addr_vec.emplace_back(ring.at(0));
			addr_vec.emplace_back(ring.at(1));
			addr_vec.emplace_back(ring.at(2));
		}
		else {
			// go through the ring until pos <= node
			for (int i=1; i<ring.size(); i++){
				Node addr = ring.at(i);
				if (pos <= addr.getHashCode()) {
					addr_vec.emplace_back(addr);
					addr_vec.emplace_back(ring.at((i+1)%ring.size()));
					addr_vec.emplace_back(ring.at((i+2)%ring.size()));
					break;
				}
			}
		}
	}
	return addr_vec;
}

/**
 * FUNCTION NAME: recvLoop
 *
 * DESCRIPTION: Receive messages from EmulNet and push into the queue (mp2q)
 */
bool MP2Node::recvLoop() {
    if ( memberNode->bFailed ) {
    	return false;
    }
    else {
    	return emulNet->ENrecv(&(memberNode->addr), this->enqueueWrapper, NULL, 1, &(memberNode->mp2q));
    }
}

/**
 * FUNCTION NAME: enqueueWrapper
 *
 * DESCRIPTION: Enqueue the message from Emulnet into the queue of MP2Node
 */
int MP2Node::enqueueWrapper(void *env, char *buff, int size) {
	Queue q;
	return q.enqueue((queue<q_elt> *)env, (void *)buff, size);
}
/**
 * FUNCTION NAME: stabilizationProtocol
 *
 * DESCRIPTION: This runs the stabilization protocol in case of Node joins and leaves
 * 				It ensures that there always 3 copies of all keys in the DHT at all times
 * 				The function does the following:
 *				1) Ensures that there are three "CORRECT" replicas of all the keys in spite of failures and joins
 *				Note:- "CORRECT" replicas implies that every key is replicated in its two neighboring nodes in the ring
 */
void MP2Node::stabilizationProtocol() {
	/*
	 * Implement this
	 */
	 
	uint i;
	for(i = 0; i < ring.size(); i++){
		if(ring.at(i).nodeAddress == memberNode->addr){
			break;
		}
	}
	
	vector<Node> r;
	r.emplace_back(ring.at((i+1)%ring.size())); 
	r.emplace_back(ring.at((i+2)%ring.size())); 
	
	if (hasMyReplicas.size() < 2 ||
		  !(r.at(0).nodeAddress == hasMyReplicas.at(0).nodeAddress &&
			  r.at(1).nodeAddress == hasMyReplicas.at(1).nodeAddress)) {
		for(auto const& it : ht->hashTable) {
			dispatchMessages(CREATE, it.first, it.second);
		}
		hasMyReplicas = r;
	}
}

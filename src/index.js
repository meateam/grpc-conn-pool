const grpc = require('@grpc/grpc-js');
const uniqid = require('uniqid');

const { loadSync } = require('@grpc/proto-loader');

class GrpcClient {
    pool = [];
    url;
    client;

    constructor(protoFile, options) {
        if(options.connCount === 0) throw new Error("connection count must be a possitive number");

        // gRPC-Server URL
        this.url = options.serverUrl ? options.serverUrl : 'localhost:50001';

        // gRPC Client Channel
        const packageDefinition = loadSync(protoFile, {
            keepCase: true,
            longs: String,
            enums: String,
            defaults: true,
            oneofs: true,
        });
        this.client = grpc.loadPackageDefinition(packageDefinition)[options.packageName][options.serviceName];

        // Setup connections in the pool
        this.setUpConnections(options.connCount || 1);

        // Initialize RPC Methods by using the First Created Client
        this.initializeMethods(this.pool[0]);
    }

    /**
     * Creates a New Connection and Adds it to the pool in FREE status
     */
    createNewConn() {
        this.pool.push({
            conn: new this.client(this.url, grpc.credentials.createInsecure()),
            id: uniqid()
        });
    }

    /**
     * Creates the recived number of connections
     */ 
    setUpConnections(num) {
        // Create all connections
        while (this.pool.length < num) {
            this.createNewConn();
        }
    }

    /**
     * Closes the client connection, and replace it.
     * @param connObj connection object
     */
    replaceClosedConn(connObj) {
        // Removes connection object from the pool
        this.pool = this.pool.filter(conn => conn.id !== connObj.id);

        // Close the client connection
        grpc.closeClient(connObj.conn);

        // create a new connection and push it to the pool
        this.createNewConn();
    }

    /**
     * Returns the first connection and circulate the pool array in order to load balance
     */
    getConn() {
        const tmp = this.pool.shift();
        if (tmp) this.pool.push(tmp);

        return tmp;
    }

    /**
     * Adds Methods from protoBuf file to `this` instance object
     * @param {ConnObj} connObj
     */
    initializeMethods(connObj) {
        for (const rpc in connObj.conn) {
            // Creating Method on `this` instance => rpc_method
            this[rpc] = async (data, callback) => {
                const freeConnObj = this.getConn();

                return new Promise((resolve, reject) => {
                    freeConnObj.conn[rpc](data, (err, res) => {
                        // Close connection if failed to connect to all addresses
                        if (err & err.code === 14) this.replaceClosedConn(freeConnObj);

                        if (callback) callback(err, res);

                        return err ? reject(err) : resolve(res);
                    });
                });
            };
        }
    }
}

module.exports = GrpcClient;
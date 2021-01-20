import * as grpc from 'grpc';
import { loadSync, PackageDefinition } from '@grpc/proto-loader';

const uniqid = require('uniqid');

interface Options {
    serviceName: string,
    serverUrl?: string,
    connCount?: number
}

interface Connection {
    conn: any,
    id: number
}

export default class grpcClient {

    private connCount: number;
    private url: string;
    private pool: Connection[];
    private client: any;

    constructor(protoFile: string, options: Options) {
        // Max Client connections to Server
        this.connCount = options.connCount ? options.connCount : 1;

        // gRPC-Server URL
        this.url = options.serverUrl ? options.serverUrl : 'localhost:50001';

        // gRPC Client Channel
        const packageDefinition: PackageDefinition = loadSync(protoFile, {
            keepCase: true,
            longs: String,
            enums: String,
            defaults: true,
            oneofs: true,
        });
        this.client = grpc.loadPackageDefinition(packageDefinition)[options.serviceName];

        this.pool = [];
        this.setUpConnections();

        // Initialize RPC Methods by using the First Created Client
        this.initializeMethods(this.pool[0]);
    }

    // Creates the recived connections 
    private setUpConnections() {
        // Create all connections
        while (this.pool.length < this.connCount) {
            this.createNewConn();
        }
    }

    /**
     * Creates a New Connection and Adds it to the pool in FREE status
     */
    private createNewConn() {
        this.pool.push({
            conn: new this.client(this.url, grpc.credentials.createInsecure()),
            id: uniqid()
        });
    }

    /**
     * Closes the client connection, and replace it.
     * @param connObj connection object
     */
    private replaceClosedConn(connObj: Connection) {
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
    private getConn(): Connection | undefined {
        const tmp = this.pool.shift();
        if (tmp) this.pool.push(tmp);

        return tmp;
    }

    /**
     * Adds Methods from protoBuf file to `this` instance object
     * @param {ConnObj} connObj
     */
    private initializeMethods(connObj: Connection) {
        for (const rpc in connObj.conn) {
            // Creating Method on `this` instance => rpc_method
            this[rpc] = async (data: any, callback?: Function) => {
                const freeConnObj = this.getConn();
                if (!freeConnObj) throw new Error();

                return new Promise((resolve, reject) => {
                    const response = freeConnObj.conn[rpc](data, (err: grpc.ServiceError, res: any) => {
                        // Close connection if failed to connect to all addresses
                        if (err.code === 14) this.replaceClosedConn(freeConnObj);

                        if (callback) callback(err, response);

                        return err ? reject(err) : resolve(res);
                    });
                });
            };
        }
    }
}
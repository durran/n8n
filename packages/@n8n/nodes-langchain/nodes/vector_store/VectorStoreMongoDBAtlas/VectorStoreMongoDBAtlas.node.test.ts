import { mock } from 'jest-mock-extended';
import { MongoClient } from 'mongodb';
import type { ILoadOptionsFunctions } from 'n8n-workflow';

import {
	EMBEDDING_NAME,
	getCollectionName,
	getEmbeddingFieldName,
	getMetadataFieldName,
	getMongoClient,
	getVectorIndexName,
	METADATA_FIELD_NAME,
	MONGO_CLIENTS,
	MONGODB_COLLECTION_NAME,
	VECTOR_INDEX_NAME,
} from './VectorStoreMongoDBAtlas.node';

describe('VectorStoreMongoDBAtlas', () => {
	const helpers = mock<ILoadOptionsFunctions['helpers']>();
	const executeFunctions = mock<ILoadOptionsFunctions>({ helpers });

	beforeEach(() => {
		jest.resetAllMocks();
	});

	describe('.getMongoClient', () => {
		describe('when no client exists for the node', () => {
			let client: MongoClient;

			beforeEach(async () => {
				executeFunctions.getNode.mockReturnValue({
					id: 'testNodeId',
					typeVersion: 1,
					name: 'testNode',
					type: 'test.node',
					position: [0, 0],
					parameters: {},
				});
				executeFunctions.getCredentials.mockResolvedValue({
					connectionString: 'mongodb://localhost:27017/admin',
					database: 'testDatabase',
				});
				client = await getMongoClient(executeFunctions);
			});

			afterEach(() => {
				MONGO_CLIENTS.clear();
			});

			it('returns a new client', () => {
				expect(client).toBeInstanceOf(MongoClient);
			});

			it('stores the client in the client map', () => {
				expect(MONGO_CLIENTS.get('testNodeId')).toBe(client);
			});

			it('does not create additional clients', () => {
				expect(MONGO_CLIENTS.size).toEqual(1);
			});
		});
	});

	describe('.getCollectionName', () => {
		beforeEach(() => {
			executeFunctions.getNodeParameter.mockImplementation((paramName: string) => {
				if (paramName === MONGODB_COLLECTION_NAME) return 'testCollection';
			});
		});

		it('returns the collection name from the context', () => {
			expect(getCollectionName(executeFunctions, 0)).toEqual('testCollection');
		});
	});

	describe('.getVectorIndexName', () => {
		beforeEach(() => {
			executeFunctions.getNodeParameter.mockImplementation((paramName: string) => {
				if (paramName === VECTOR_INDEX_NAME) return 'testIndex';
			});
		});

		it('returns the index name from the context', () => {
			expect(getVectorIndexName(executeFunctions, 0)).toEqual('testIndex');
		});
	});

	describe('.getEmbeddingFieldName', () => {
		beforeEach(() => {
			executeFunctions.getNodeParameter.mockImplementation((paramName: string) => {
				if (paramName === EMBEDDING_NAME) return 'testEmbedding';
			});
		});

		it('returns the embedding name from the context', () => {
			expect(getEmbeddingFieldName(executeFunctions, 0)).toEqual('testEmbedding');
		});
	});

	describe('.getMetadataFieldName', () => {
		beforeEach(() => {
			executeFunctions.getNodeParameter.mockImplementation((paramName: string) => {
				if (paramName === METADATA_FIELD_NAME) return 'testMetadata';
			});
		});

		it('returns the metadata field name from the context', () => {
			expect(getMetadataFieldName(executeFunctions, 0)).toEqual('testMetadata');
		});
	});
});

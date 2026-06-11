import { InvokeCommand, InvokeCommandInput, } from '@aws-sdk/client-lambda';
import { client, clientWest } from './monitoringLambdaClient.js';
import { safeParseJSON } from '../../../utils/general.js';

const invokeLambda = async (params: InvokeCommandInput): Promise<unknown> => {
  const invokeCommand = new InvokeCommand(params);
  try {
    if (params.FunctionName && params.FunctionName.includes('us-west-2')) {
      const result = await clientWest.send(invokeCommand);

      if (result.FunctionError) {
        const payload = result.Payload
          ? safeParseJSON(result.Payload.transformToString('utf-8'))
          : null;
        console.error(`Error occured during west function execution with params: ${JSON.stringify(params)}`, JSON.stringify(payload));
        return Promise.reject(
          Error('Error occured during west function execution', { cause: payload })
        );
      }

      return result.Payload ? safeParseJSON(result.Payload.transformToString('utf-8')) : null;
    }

    const result = await client.send(invokeCommand);
    console.log(result);

    if (result.FunctionError) {
      const payload = result.Payload
        ? safeParseJSON(result.Payload.transformToString('utf-8'))
        : null;
      console.error(`Error occured during function execution with params: ${JSON.stringify(params)}`, JSON.stringify(payload));
      return Promise.reject(
        Error('Error occured during function execution', { cause: payload })
      );
    }

    return result.Payload ? safeParseJSON(result.Payload.transformToString('utf-8')) : null;
  } catch (err) {
    console.error(`Error during invokeLambda operation with params: ${JSON.stringify(params)}`, err);
    return Promise.reject(err);
  }
};

export { invokeLambda };

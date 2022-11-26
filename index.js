'use strict'

import Message_Queue from 'amqplib'
import {v4 as uuid} from 'uuid'
import {promisify} from 'util'
import config from 'config'
import cluster from 'cluster'

let reply_queue

const old_log = console.log
console.log = function ()
{
	const now = new Date(Date.now())
	const year = now.getFullYear()
	const month = `00${now.getMonth() + 1}`.slice(-2)
	const day = `00${now.getDate()}`.slice(-2)
	const hour = `00${now.getHours()}`.slice(-2)
	const minute = `00${now.getMinutes()}`.slice(-2)
	const second = `00${now.getSeconds()}`.slice(-2)
	const millisecond = `000${now.getMilliseconds()}`.slice(-3)

	const timestamp = `[${year}-${month}-${day} ${hour}:${minute}:${second}.${millisecond}]`

	arguments[0] = `${timestamp} ${arguments[0]}`
	old_log(...arguments)
}

if (cluster.isPrimary)
{
	cluster.fork()

	cluster.on('exit', (worker, code, signal) =>
	{
		console.log(`Worker exited with code: ${code} via signal: ${signal}`)
		cluster.fork()
	})
}
else
{
	try
	{	
		const sleep = promisify(setTimeout)
		const timeout = config.get('general.timeout')
		const rabbit_config = config.get('rabbit')
		const queue_name = config.get('general.sign_up_queue')
		const validator_queue = config.get('general.validator_queue')
		const mail_queue = config.get('general.mail_queue')
		
		const connection = await Message_Queue.connect(rabbit_config)
		
		const channel = await connection.createChannel()
		channel.prefetch(1)
	
		process.once('sigint', () =>
		{
			connection.close()
		})
		
		await channel.assertQueue(queue_name)
		channel.consume(queue_name, queue_work)
		reply_queue = await channel.assertQueue('', {durable: false, exclusive: true})
		
		async function queue_work(incoming_message)
		{
			try
			{
				const data = JSON.parse(incoming_message.content.toString())
	
				if(data == null)
				{
					channel.nack(incoming_message, false, false)
					console.log(`Data is null for message ${incoming_message}`)
				}
				else
				{
					let first_name = data.first_name
					let last_name = data.last_name
					let display_name = data.display_name
					let email = data.email
					let password = data.password
					let birthday = data.birthday
		
					let correlation_id = uuid()
		
					let validation = {}
		
					validation.first_name = await validate_entry('name', first_name, correlation_id, reply_queue)
					validation.last_name = await validate_entry('name', last_name, correlation_id, reply_queue)
					validation.display_name = await validate_entry('display_name', display_name, correlation_id, reply_queue)
					validation.email = await validate_entry('email', email, correlation_id, reply_queue)
					validation.password = await validate_entry('password', password, correlation_id, reply_queue)
					validation.birthday = await validate_entry('date', birthday, correlation_id, reply_queue)
		
					let message = []
					let errors = 0
					let code = 200
					for(const [key, value] of Object.entries(validation))
					{
						if(!value.success)
						{
							errors++
							code = Math.max(code, value.code)
						}
		
						message.push(
							[
								key,
								{
									code: value.code,
									message: value.message
								}
							])
					}
					
					let final_results =
					{
						code,
						message
					}
					if(errors > 0)
					{
						// Don't touch the database
					}
					else
					{
						// Sign up like normal

						const verification_code = generate_verification_code()
						const mail_data =
						{
							type: 'verify',
							display_name,
							verification_code,
							email
						}
						await channel.sendToQueue(
							mail_queue,
							Buffer.from(JSON.stringify(mail_data))
						)
					}
		
					//console.log(util.inspect(results, true, Infinity))
					
					await channel.sendToQueue(
						incoming_message.properties.replyTo,
						Buffer.from(JSON.stringify(final_results)),
						{correlationId: incoming_message.properties.correlationId}
					)
		
					channel.ack(incoming_message)
				}
			}
			catch(e)
			{
				console.log(e)
				process.exit(1)
			}
		}
	
		async function validate_entry(type, input, correlation_id, reply_queue)
		{
			let data =
			{
				type,
				input
			}
	
			await channel.sendToQueue(
				validator_queue,
				Buffer.from(JSON.stringify(data)),
				{
					correlation_id,
					replyTo: reply_queue.queue
				}
			)
	
			let start_time = process.hrtime.bigint()
			let output
			
			while(true)
			{
				let message = await channel.get(reply_queue.queue)
				if(message !== false)
				{
					output = JSON.parse(message.content.toString())
					channel.ack(message)
					break
				}
				await sleep(100)
				if((process.hrtime.bigint() - start_time) > timeout)
				{
					throw `Timeout waiting for ${type} validation response`
				}
			}
			
			return output
		}

		function generate_verification_code()
		{
			return (uuid()).replaceAll('-', '')
		}
	}
	catch(e)
	{
		console.log(e)
		process.exit(2)
	}
}

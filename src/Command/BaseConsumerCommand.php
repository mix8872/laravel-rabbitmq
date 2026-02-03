<?php
namespace NeedleProject\LaravelRabbitMq\Command;

use Illuminate\Console\Command;
use Illuminate\Support\Facades\Log;
use Monolog\Handler\StreamHandler;
use Monolog\Logger;
use NeedleProject\LaravelRabbitMq\ConsumerInterface;
use Psr\Log\LoggerAwareInterface;
use Psr\Log\LoggerInterface;
use Symfony\Component\Console\Output\OutputInterface;

/**
 * Class BaseConsumerCommand
 *
 * @package NeedleProject\LaravelRabbitMq\Command
 * @author  Adrian Tilita <adrian@tilita.ro>
 */
class BaseConsumerCommand extends Command
{
    /**
     * The name and signature of the console command.
     *
     * @var string
     */
    protected $signature = 'rabbitmq:consume {consumer} {--time=60} {--messages=100} {--memory=64}';

    /**
     * The console command description.
     *
     * @var string
     */
    protected $description = 'Start consuming messages';

    /**
     * @param string $consumerAliasName
     * @return ConsumerInterface
     */
    protected function getConsumer(string $consumerAliasName): ConsumerInterface
    {
        return app()->make(ConsumerInterface::class, [$consumerAliasName]);
    }

    public function handle()
    {
        rmq_log("[RMQ BaseConsumerCommand] handle() called");
        
        $messageCount = (int)$this->input->getOption('messages');
        $waitTime = (int)$this->input->getOption('time');
        $memoryLimit = (int)$this->input->getOption('memory');
        $consumerName = $this->input->getArgument('consumer');
        
        rmq_log(sprintf(
            "[RMQ BaseConsumerCommand] Parameters parsed: consumer=%s, messages=%s, time=%s, memory=%sMB",
            $consumerName,
            $messageCount,
            $waitTime,
            $memoryLimit
        ));
        
        $isVerbose = in_array(
            $this->output->getVerbosity(),
            [OutputInterface::VERBOSITY_VERBOSE, OutputInterface::VERBOSITY_VERY_VERBOSE]
        );

        rmq_log(sprintf("[RMQ BaseConsumerCommand] Verbose mode: %s", $isVerbose ? 'true' : 'false'));

        $this->info("Initializing consumer: {$consumerName}");
        $this->info("Parameters: messages={$messageCount}, time={$waitTime}, memory={$memoryLimit}MB");

        try {
            rmq_log("[RMQ BaseConsumerCommand] Creating consumer instance...");
            /** @var ConsumerInterface $consumer */
            $consumer = $this->getConsumer($consumerName);
            rmq_log(sprintf("[RMQ BaseConsumerCommand] Consumer created: %s", get_class($consumer)));
            $this->info("Consumer created successfully");
            
            if ($consumer instanceof LoggerAwareInterface && $isVerbose) {
                rmq_log("[RMQ BaseConsumerCommand] Attempting to inject CLI logger...");
                try {
                    $this->injectCliLogger($consumer);
                    rmq_log("[RMQ BaseConsumerCommand] CLI logger injected successfully");
                } catch (\Throwable $e) {
                    rmq_log(sprintf("[RMQ BaseConsumerCommand] Failed to inject CLI logger: %s", $e->getMessage()));
                    // Do nothing, we cannot inject a STDOUT logger
                }
            } else {
                rmq_log(sprintf(
                    "[RMQ BaseConsumerCommand] Skipping CLI logger: LoggerAwareInterface=%s, isVerbose=%s",
                    $consumer instanceof LoggerAwareInterface ? 'true' : 'false',
                    $isVerbose ? 'true' : 'false'
                ));
            }
            
            $this->info("Starting to consume messages...");
            rmq_log("[RMQ BaseConsumerCommand] Calling startConsuming()...");
            
            // Add debug output if consumer supports reflection
            if ($consumer instanceof LoggerAwareInterface) {
                try {
                    rmq_log("[RMQ BaseConsumerCommand] Inspecting consumer via reflection...");
                    $reflection = new \ReflectionClass($consumer);
                    if ($reflection->hasMethod('shouldStopConsuming')) {
                        $method = $reflection->getMethod('shouldStopConsuming');
                        $method->setAccessible(true);
                        $shouldStop = $method->invoke($consumer);
                        rmq_log(sprintf("[RMQ BaseConsumerCommand] Reflection: shouldStopConsuming() = %s", $shouldStop ? 'true' : 'false'));
                        $this->line("DEBUG: First shouldStopConsuming() check = " . ($shouldStop ? 'true (WILL STOP)' : 'false (WILL CONTINUE)'));
                        
                        // Try to get limit values
                        if ($reflection->hasProperty('limitMessageCount')) {
                            $prop = $reflection->getProperty('limitMessageCount');
                            $prop->setAccessible(true);
                            $limitMsg = $prop->getValue($consumer);
                            rmq_log(sprintf("[RMQ BaseConsumerCommand] Reflection: limitMessageCount = %s", $limitMsg));
                            $this->line("DEBUG: limitMessageCount = {$limitMsg}");
                        }
                        if ($reflection->hasProperty('limitSecondsUptime')) {
                            $prop = $reflection->getProperty('limitSecondsUptime');
                            $prop->setAccessible(true);
                            $limitTime = $prop->getValue($consumer);
                            rmq_log(sprintf("[RMQ BaseConsumerCommand] Reflection: limitSecondsUptime = %s", $limitTime));
                            $this->line("DEBUG: limitSecondsUptime = {$limitTime}");
                        }
                        if ($reflection->hasProperty('startTime')) {
                            $prop = $reflection->getProperty('startTime');
                            $prop->setAccessible(true);
                            $startTime = $prop->getValue($consumer);
                            rmq_log(sprintf("[RMQ BaseConsumerCommand] Reflection: startTime = %s", $startTime));
                            $this->line("DEBUG: startTime = {$startTime}");
                        }
                    }
                } catch (\Throwable $e) {
                    rmq_log(sprintf("[RMQ BaseConsumerCommand] Reflection error: %s - %s", get_class($e), $e->getMessage()));
                    $this->line("DEBUG: Could not inspect consumer: " . $e->getMessage());
                }
            }
            
            $result = $consumer->startConsuming($messageCount, $waitTime, $memoryLimit);
            rmq_log(sprintf("[RMQ BaseConsumerCommand] startConsuming() returned: %s", $result));
            $this->info("Consumer finished with code: {$result}");
            return $result;
        } catch (\Throwable $e) {
            rmq_log(sprintf(
                "[RMQ BaseConsumerCommand] EXCEPTION: %s - %s in %s:%d",
                get_class($e),
                $e->getMessage(),
                $e->getFile(),
                $e->getLine()
            ));
            rmq_log(sprintf("[RMQ BaseConsumerCommand] Stack trace: %s", $e->getTraceAsString()));
            
            $this->error("Error in consumer: " . $e->getMessage());
            $this->error("File: " . $e->getFile() . ":" . $e->getLine());
            if ($this->output->isVerbose()) {
                $this->error($e->getTraceAsString());
            }
            if (isset($consumer)) {
                try {
                    rmq_log("[RMQ BaseConsumerCommand] Attempting to stop consumer after error...");
                    $consumer->stopConsuming();
                    rmq_log("[RMQ BaseConsumerCommand] Consumer stopped");
                } catch (\Throwable $stopException) {
                    rmq_log(sprintf("[RMQ BaseConsumerCommand] Error stopping consumer: %s", $stopException->getMessage()));
                    // Ignore errors when stopping
                }
            }
            throw $e;
        }
    }

    /**
     * Inject a stdout logger
     *
     * This is a "hackish" method because we handle a interface to deduce an implementation
     * that exposes certain methods.
     *
     * @todo - Find a better way to inject a CLI logger when running in verbose mode
     *
     * @param LoggerAwareInterface $consumerWithLogger
     * @throws \Exception
     */
    protected function injectCliLogger(LoggerAwareInterface $consumerWithLogger)
    {
        $stdHandler = new StreamHandler('php://stdout');
        $class = new \ReflectionClass(get_class($consumerWithLogger));
        $property = $class->getProperty('logger');
        $property->setAccessible(true);
        /** @var LoggerInterface $logger */
        $logger = $property->getValue($consumerWithLogger);
        if ($logger instanceof \Illuminate\Log\LogManager) {
            /** @var Logger $logger */
            $logger = $logger->channel()->getLogger();
            $logger->pushHandler($stdHandler);
        }
        $property->setAccessible(false);
    }
}

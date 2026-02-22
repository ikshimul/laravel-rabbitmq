<?php

namespace App\Console\Commands;

use App\Services\RabbitMQService;
use Illuminate\Console\Command;

class RabbitMQConsumeCommand extends Command
{
    /**
     * The name and signature of the console command.
     *
     * @var string
     */
    protected $signature = 'mq:consume';

    /**
     * The console command description.
     *
     * @var string
     */
    protected $description = 'RabbitMQ consume';

    /**
     * Execute the console command.
     */
    public function handle()
    {
        $rabbitMQ = new RabbitMQService();
        
        //$rabbitMQ->consumeDirect();

        //$rabbitMQ->consumeTopic('order_queue', 'order.*');

        //$rabbitMQ->consumeFanout();

        $rabbitMQ->consumeHeaders();
    }
}

<?php

namespace App\Console\Commands;

use App\Services\RabbitMQService;
use Illuminate\Console\Command;

class RabbitMQMailConsumeCommand extends Command
{
    /**
     * The name and signature of the console command.
     *
     * @var string
     */
    protected $signature = 'mail:consume';

    /**
     * The console command description.
     *
     * @var string
     */
    protected $description = 'RabbitMQ mail consume';

    /**
     * Execute the console command.
     */
    public function handle()
    {
        $rabbitMQ = new RabbitMQService();
        
         //$rabbitMQ->consumeDirect();

         //$rabbitMQ->consumeTopic('order_queue', 'mail.*');
         
         $rabbitMQ->consumeFanout('fanout_exchange', 'mail_queue');
    }
}

<?php

use App\Services\RabbitMQService;
use Illuminate\Http\Request;
use Illuminate\Support\Facades\Route;

Route::post('/message', function (Request $request) {
    $message = $_POST['message'];
    $rabbitMQ = new RabbitMQService();

    //$rabbitMQ->publishDirect($message);
    //$rabbitMQ->publishDirect($message.' mail send','exchange_new','user_mail_queue','send_mail');

    // $rabbitMQ->publishTopic($message);
    // $rabbitMQ->publishTopic($message.' mail send','mail.send');

    //$rabbitMQ->publishFanout($message);

    $rabbitMQ->publishHeaders([
        "x-match" => "any",
        "notification-type-comment" => "comment",
        "notification-type-like" => "like",
    ], $message);



    return redirect('/');
});

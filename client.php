<?php

$fp = fsockopen('unix:///tmp/counter.sock', -1, $errno, $errstr);
if ($fp) {
    fwrite($fp, pack('V', rand(1, 1048576)));
    fclose($fp);
} else {
    echo $errstr;
}

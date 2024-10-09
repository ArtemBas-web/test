<?php

/**
 * Сервис для работы с базой данных
 */
class SpecPdoService extends PDO {
    
    /**
     * @var string
     */
    private string $dbHost = '127.0.0.1';
    
    /**
     * @var string
     */
    private string $db = 'test2';
    
    /**
     * @var string
     */
    private string $user = 'root';
    
    /**
     * @var string
     */
    private string $pass = '';
    
    /**
     * @var string
     */
    private string $charset = 'utf8mb4';
    
    /**
     *
     */
    public function __construct() {
        
        $dsn = "mysql:host=" . $this->dbHost . ";dbname=" . $this->db . ";charset=" . $this->charset;
        $options = [
            PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
            PDO::ATTR_DEFAULT_FETCH_MODE => PDO::FETCH_ASSOC,
            PDO::ATTR_EMULATE_PREPARES => FALSE,
        ];
        parent::__construct($dsn, $this->user, $this->pass, $options);
    }
    
    /**
     * @param $query
     *
     * @return ?\PDOStatement
     */
    public function runQuery($query)
    :?PDOStatement {
        
        try {
            
            return $this->query($query);
        } catch (\PDOException $e) {
            echo $e->getMessage();
            
            return NULL;
        }
    }
    
    /**
     * @param $query
     *
     * @return array
     */
    public function runQueryAndFetchAll($query)
    :array {
        
        $query = $this->runQuery($query);
        if ($query) {
            $fetch = $query->fetchAll();
            $query->closeCursor();
            
            return $fetch;
        }
        else {
            return [];
        }
    }
}


/**
 *
 */
class Test {
    
    /**
     * @var SpecPdoService|null
     */
    private ?SpecPdoService $specPdoService = NULL;
    
    /**
     * @var array
     */
    private array $allSessions = [];
    
    /**
     * @var array
     */
    private array $allClientMembers = [];
    
    /**
     *
     */
    public function __construct() {
        
        try {
            $this->specPdoService = new SpecPdoService();
        } catch (\PDOException $e) {
            throw new \PDOException($e->getMessage(), (int)$e->getCode());
        }
        
        //Тут импровизация, для логирования (в реальном проекте я бы сделал иначе)
        $this->allSessions = $this->specPdoService->runQueryAndFetchAll('SELECT * FROM sessions');
        $this->allSessions = $this->resetKeys($this->allSessions);
        $this->allClientMembers = $this->specPdoService->runQueryAndFetchAll('SELECT * FROM session_members');
        $this->allClientMembers = $this->resetKeys($this->allClientMembers);
    }
    
    /**
     * Изменение ключей массива на id
     *
     * @param array $array
     *
     * @return array
     */
    private function resetKeys(array $array)
    :array {
        
        $newArray = [];
        foreach ($array as $row) {
            if (isset($row['id'])) {
                $newArray[$row['id']] = $row;
            }
        }
        
        return $newArray;
    }
    
    /**
     * Стек логов
     * @var array
     */
    private $logs = [];
    
    /**
     * Установка лога
     *
     * @param string $message
     *
     * @return void
     */
    private function setLog(string $message)
    :void {
        
        $this->logs[] = $message . '<br>';
        // echo $message . '<br>';
    }
    
    /**
     * Вывод логов
     * @return void
     */
    private function echoLogs()
    :void {
        
        echo count($this->logs) . ' логов <br>';
        foreach ($this->logs as $log) {
            echo $log;
        }
    }
    
    /**
     * Получение всех клиентов
     * @return array|null
     */
    private function getAllClients()
    :?array {
        
        return $this->specPdoService->runQueryAndFetchAll('SELECT id FROM clients');
    }
    
    /**
     * Получение всех уникальных занятий (правильных)
     * @return array|null
     */
    private function getDistinctLessons()
    :?array {
        
        return $this->specPdoService->runQueryAndFetchAll('SELECT DISTINCT start_time, session_configuration_id FROM sessions');
    }
    
    /**
     * Получение списка занятий по id конфигурации и времени начала
     *
     * @param int $session_configuration_id
     * @param string $start_time
     *
     * @return array|null
     */
    private function getSessionListByConfigurationIdAndStartTime(int $session_configuration_id, string $start_time)
    :?array {
        
        return $this->specPdoService->runQueryAndFetchAll('SELECT * FROM sessions WHERE session_configuration_id = ' . $session_configuration_id . " AND start_time = '" . $start_time . "'");
    }
    
    /**
     * Выборка оригинала
     *
     * @param array $all
     *
     * @return int|null
     */
    private function getOriginal(array $all)
    :?int {
        
        return $all[0] ?? NULL;
    }
    
    /**
     * Выборка дубликатов
     *
     * @param array $all
     *
     * @return array|null
     */
    private function getDublicates(array $all)
    :?array {
        
        return array_slice($all, 1);
    }
    
    /**
     * Удаление дубликатов
     *
     * @param array $dublicates
     *
     * @return void
     */
    private function deleteDublicates(array $dublicates)
    :void {
        
        if (!empty($dublicates))
            $this->specPdoService->runQuery('DELETE FROM sessions WHERE id IN (' . implode(', ', $dublicates) . ')');
        
        foreach ($dublicates as $dublicate) {
            $session = $this->allSessions[$dublicate];
            $this->setLog('Удаление дубликата занятия (session): ' . 'id = ' . $session['id'] . ' session_configuration_id = ' . $session['session_configuration_id'] . ' start_time = ' . $session['start_time']);
        }
        
    }
    
    /**
     * Получение всех посящений пользователя по занятиям
     *
     * @param int $client_id
     * @param array $all
     *
     * @return array|null
     */
    private function getUserMembers(int $client_id, array $all)
    :?array {
        
        return $this->specPdoService->runQueryAndFetchAll('SELECT * FROM session_members WHERE client_id = ' . $client_id . ' AND session_id IN (' . implode(', ', $all) . ')');
    }
    
    /**
     * Удаление дубликатов отметок о посещении
     *
     * @param int $id
     *
     * @return void
     */
    private function deleteSessionMembers(int $id)
    :void {
        
        $this->specPdoService->runQuery('DELETE FROM session_members WHERE id = ' . $id);
        $sessionMember = $this->allClientMembers[$id];
        $this->setLog('Удаление дубликата отметки о посещении (session_members) ' . 'session_id= ' . $sessionMember['session_id'] . ' client_id= ' . $sessionMember['client_id']);
    }
    
    /**
     * Добавление отметки о посещении (оригинала)
     *
     * @param int $client_id
     * @param int $original
     *
     * @return void
     */
    private function insertSessionMembers(int $client_id, int $original)
    :void {
        
        $this->specPdoService->runQuery('INSERT INTO session_members (client_id, session_id) VALUES (' . $client_id . ', ' . $original . ')');
    }
    
    /**
     * Перебор всех клиентов и удаление дубликатов отметок о посещении
     *
     * @param array $clients
     * @param array $all
     * @param array $dublicates
     * @param int $original
     *
     * @return void
     */
    private function deleteAllUserMemberDublicates(array $clients, array $all, array $dublicates, int $original)
    :void {
        
        foreach ($clients as $client) {
            $userMembers = $this->getUserMembers($client['id'], $all);
            $ids = array_column($userMembers, 'session_id');
            $isDublicate = FALSE;
            foreach ($userMembers as $userMember) {
                if (in_array($userMember['session_id'], $dublicates)) {
                    $isDublicate = TRUE;
                    $this->deleteSessionMembers($userMember['id']);
                }
            }
            if ($isDublicate && !in_array($original, $ids)) {
                $this->insertSessionMembers($client['id'], $original);
            }
        }
    }
    
    /**
     * Основной метод
     * @return void
     */
    public function run()
    :void {
        
        $clients = $this->getAllClients();
        $lessons = $this->getDistinctLessons();
        foreach ($lessons as $lesson) {
            $sessions = $this->getSessionListByConfigurationIdAndStartTime($lesson['session_configuration_id'], $lesson['start_time']);
            $all = array_column($sessions, 'id');
            $original = $this->getOriginal($all);
            $dublicates = $this->getDublicates($all);
            $this->deleteAllUserMemberDublicates($clients, $all, $dublicates, $original);
            $this->deleteDublicates($dublicates);
        }
        $this->echoLogs();
    }
}

$test = new Test();
$test->run();

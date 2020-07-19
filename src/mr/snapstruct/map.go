package snapstruct
import "sync"

type Map struct {
	internalMap map[interface{}]interface{}
	mutex sync.Mutex
}

func NewMap() *Map {
	return &Map{
		make(map[interface{}]interface{}),
		sync.Mutex{},
	}
}

func (m *Map) Set(
	key interface{},
	value interface{},
) {
	m.SetAllFrom(map[interface{}]interface{}{key: value})
}

func (m* Map) SetAllFrom(
	other map[interface{}]interface{},
) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	copyMap(
		other,
		m.internalMap,
	)
}

func (m *Map) Snapshot() map[interface{}]interface{} {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	
	return copyMap(
		m.internalMap, 
		make(map[interface{}]interface{}),
	)
}

func copyMap(
	source map[interface{}]interface{}, 
	target map[interface{}]interface{},
) map[interface{}]interface{} {
	for key, value := range source {
		target[key] = value
	} 
	return target
}
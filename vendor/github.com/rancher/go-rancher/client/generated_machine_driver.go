package client

const (
	MACHINE_DRIVER_TYPE = "machineDriver"
)

type MachineDriver struct {
	Resource

	AccountId string `json:"accountId,omitempty" yaml:"account_id,omitempty"`

	Created string `json:"created,omitempty" yaml:"created,omitempty"`

	Data map[string]interface{} `json:"data,omitempty" yaml:"data,omitempty"`

	Description string `json:"description,omitempty" yaml:"description,omitempty"`

	ErrorMessage string `json:"errorMessage,omitempty" yaml:"error_message,omitempty"`

	Kind string `json:"kind,omitempty" yaml:"kind,omitempty"`

	Md5checksum string `json:"md5checksum,omitempty" yaml:"md5checksum,omitempty"`

	Name string `json:"name,omitempty" yaml:"name,omitempty"`

	RemoveTime string `json:"removeTime,omitempty" yaml:"remove_time,omitempty"`

	Removed string `json:"removed,omitempty" yaml:"removed,omitempty"`

	State string `json:"state,omitempty" yaml:"state,omitempty"`

	Transitioning string `json:"transitioning,omitempty" yaml:"transitioning,omitempty"`

	TransitioningMessage string `json:"transitioningMessage,omitempty" yaml:"transitioning_message,omitempty"`

	TransitioningProgress int64 `json:"transitioningProgress,omitempty" yaml:"transitioning_progress,omitempty"`

	Uri string `json:"uri,omitempty" yaml:"uri,omitempty"`

	Uuid string `json:"uuid,omitempty" yaml:"uuid,omitempty"`
}

type MachineDriverCollection struct {
	Collection
	Data []MachineDriver `json:"data,omitempty"`
}

type MachineDriverClient struct {
	rancherClient *RancherClient
}

type MachineDriverOperations interface {
	List(opts *ListOpts) (*MachineDriverCollection, error)
	Create(opts *MachineDriver) (*MachineDriver, error)
	Update(existing *MachineDriver, updates interface{}) (*MachineDriver, error)
	ById(id string) (*MachineDriver, error)
	Delete(container *MachineDriver) error

	ActionActivate(*MachineDriver) (*MachineDriver, error)

	ActionCreate(*MachineDriver) (*MachineDriver, error)

	ActionError(*MachineDriver, *MachineDriverErrorInput) (*MachineDriver, error)

	ActionPurge(*MachineDriver) (*MachineDriver, error)

	ActionRemove(*MachineDriver) (*MachineDriver, error)

	ActionUpdate(*MachineDriver, *MachineDriverUpdateInput) (*MachineDriver, error)
}

func newMachineDriverClient(rancherClient *RancherClient) *MachineDriverClient {
	return &MachineDriverClient{
		rancherClient: rancherClient,
	}
}

func (c *MachineDriverClient) Create(container *MachineDriver) (*MachineDriver, error) {
	resp := &MachineDriver{}
	err := c.rancherClient.doCreate(MACHINE_DRIVER_TYPE, container, resp)
	return resp, err
}

func (c *MachineDriverClient) Update(existing *MachineDriver, updates interface{}) (*MachineDriver, error) {
	resp := &MachineDriver{}
	err := c.rancherClient.doUpdate(MACHINE_DRIVER_TYPE, &existing.Resource, updates, resp)
	return resp, err
}

func (c *MachineDriverClient) List(opts *ListOpts) (*MachineDriverCollection, error) {
	resp := &MachineDriverCollection{}
	err := c.rancherClient.doList(MACHINE_DRIVER_TYPE, opts, resp)
	return resp, err
}

func (c *MachineDriverClient) ById(id string) (*MachineDriver, error) {
	resp := &MachineDriver{}
	err := c.rancherClient.doById(MACHINE_DRIVER_TYPE, id, resp)
	if apiError, ok := err.(*ApiError); ok {
		if apiError.StatusCode == 404 {
			return nil, nil
		}
	}
	return resp, err
}

func (c *MachineDriverClient) Delete(container *MachineDriver) error {
	return c.rancherClient.doResourceDelete(MACHINE_DRIVER_TYPE, &container.Resource)
}

func (c *MachineDriverClient) ActionActivate(resource *MachineDriver) (*MachineDriver, error) {

	resp := &MachineDriver{}

	err := c.rancherClient.doAction(MACHINE_DRIVER_TYPE, "activate", &resource.Resource, nil, resp)

	return resp, err
}

func (c *MachineDriverClient) ActionCreate(resource *MachineDriver) (*MachineDriver, error) {

	resp := &MachineDriver{}

	err := c.rancherClient.doAction(MACHINE_DRIVER_TYPE, "create", &resource.Resource, nil, resp)

	return resp, err
}

func (c *MachineDriverClient) ActionError(resource *MachineDriver, input *MachineDriverErrorInput) (*MachineDriver, error) {

	resp := &MachineDriver{}

	err := c.rancherClient.doAction(MACHINE_DRIVER_TYPE, "error", &resource.Resource, input, resp)

	return resp, err
}

func (c *MachineDriverClient) ActionPurge(resource *MachineDriver) (*MachineDriver, error) {

	resp := &MachineDriver{}

	err := c.rancherClient.doAction(MACHINE_DRIVER_TYPE, "purge", &resource.Resource, nil, resp)

	return resp, err
}

func (c *MachineDriverClient) ActionRemove(resource *MachineDriver) (*MachineDriver, error) {

	resp := &MachineDriver{}

	err := c.rancherClient.doAction(MACHINE_DRIVER_TYPE, "remove", &resource.Resource, nil, resp)

	return resp, err
}

func (c *MachineDriverClient) ActionUpdate(resource *MachineDriver, input *MachineDriverUpdateInput) (*MachineDriver, error) {

	resp := &MachineDriver{}

	err := c.rancherClient.doAction(MACHINE_DRIVER_TYPE, "update", &resource.Resource, input, resp)

	return resp, err
}

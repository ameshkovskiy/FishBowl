import pytest
import copy

from fish_bowl.dataio.persistence import SimulationClient
from fish_bowl.process.base import SimulationGrid
from fish_bowl.process.topology import SquareGridCoordinate
from fish_bowl.process.utils import Animal
from fish_bowl.process.utils import EndOfSimulatioError

sim_config = {
    'grid_size': 10,
    'init_nb_fish': 50,
    'fish_breed_maturity': 3,
    'fish_breed_probability': 80,
    'fish_speed': 2,
    'init_nb_shark': 5,
    'shark_breed_maturity': 5,
    'shark_breed_probability': 100,
    'shark_speed': 4,
    'shark_starving': 4}

sim_config_empty = {
    'grid_size': 10,
    'init_nb_fish': 0,
    'fish_breed_maturity': 3,
    'fish_breed_probability': 100,
    'fish_speed': 2,
    'init_nb_shark': 0,
    'shark_breed_maturity': 3,
    'shark_breed_probability': 100,
    'shark_speed': 4,
    'shark_starving': 4}




class TestBase:

    def test_simulation_init(self):
        client = SimulationClient('sqlite:///:memory:')
        grid = SimulationGrid(persistence=client, simulation_parameters=sim_config)
        # Should have spawned the fishes and sharks
        nb_fish = sim_config['init_nb_fish']
        nb_sharks = sim_config['init_nb_shark']
        grid_table = grid._persistence.get_animals_df(grid._sid)
        assert len(grid_table) == (nb_fish + nb_sharks), 'Missing some animals!'

    def test_starving(self):
        client = SimulationClient('sqlite:///:memory:')
        grid = SimulationGrid(persistence=client, simulation_parameters=sim_config)
        turn_to_starve = sim_config['shark_starving']
        # set turn to more...
        grid._sim_turn = turn_to_starve + 1
        grid._check_deads()
        assert len(grid._persistence.get_animals_by_type(grid._sid, Animal.Shark)) == 0

        # amend this test by adding further checks
        # after the first turn we should have killed the sharks
        with pytest.raises(EndOfSimulatioError):
            grid.check_simulation_ends()

        with pytest.raises(EndOfSimulatioError):
            grid._check_deads()

        with pytest.raises(EndOfSimulatioError):
            grid.play_turn()

    def test_eating(self):
        client = SimulationClient('sqlite:///:memory:')
        grid = SimulationGrid(persistence=client, simulation_parameters=sim_config_empty)
        a_list = [
            (Animal.Fish, SquareGridCoordinate(x=1, y=1)),
            (Animal.Fish, SquareGridCoordinate(x=2, y=1)),
            (Animal.Fish, SquareGridCoordinate(x=3, y=1)),
            (Animal.Fish, SquareGridCoordinate(x=1, y=3)),
            (Animal.Fish, SquareGridCoordinate(x=3, y=2)),
            (Animal.Shark, SquareGridCoordinate(x=2, y=2))
        ]
        # adding some food and a shark
        for t, c in a_list:
            client.init_animal(sim_id=grid._sid, current_turn=0, animal_type=t, coordinate=c)
        # updating grid turn
        grid._sim_turn = 4
        # AM: the next two rows are to allow optimized 'occupied coordinates' check
        grid.animals = grid.get_simulation_grid_data()
        grid.occupied_coord = set(zip(grid.animals.coord_x, grid.animals.coord_y))
        shark_update = grid._eat()
        # shark has eaten
        shark = grid._persistence.get_animals_by_type(sim_id=grid._sid, animal_type=Animal.Shark).iloc[0]
        # the shark is in update list
        assert len(shark_update) == 1, 'There should be one shark in update list'
        assert shark.last_fed == 4, 'Shark last fed value should have updated'
        assert shark_update[shark.oid] == SquareGridCoordinate(x=2, y=2), 'Shark previous coordinate in shark update'
        # Fish is dead
        animal_in_square = client.get_animal_in_position(sim_id=grid._sid,
                                                         coordinate=SquareGridCoordinate(int(shark.coord_x),
                                                                                         int(shark.coord_y)),
                                                         live_only=False)
        assert len(animal_in_square) == 2, 'there shoud be two animals in that square'

    def test_breed(self):
        client = SimulationClient('sqlite:///:memory:')
        grid = SimulationGrid(persistence=client, simulation_parameters=sim_config_empty)
        a_list = [
            (Animal.Fish, SquareGridCoordinate(x=1, y=1)),
            (Animal.Fish, SquareGridCoordinate(x=2, y=1)),
            (Animal.Fish, SquareGridCoordinate(x=3, y=1)),
            (Animal.Fish, SquareGridCoordinate(x=1, y=3)),
            (Animal.Fish, SquareGridCoordinate(x=3, y=2)),
            (Animal.Shark, SquareGridCoordinate(x=2, y=2))
        ]
        # adding some food and a shark
        for t, c in a_list:
            client.init_animal(sim_id=grid._sid, current_turn=0, animal_type=t, coordinate=c)
        # updating grid turn
        grid._sim_turn = 4
        # AM: the next two rows are to allow optimized 'occupied coordinates' check
        grid.animals = grid.get_simulation_grid_data()
        grid.occupied_coord = set(zip(grid.animals.coord_x, grid.animals.coord_y))
        shark_update = grid._eat()
        assert len(shark_update) == 1, 'Shark should have fed'
        breed_moved = grid._breed_and_move(fed_sharks=shark_update)
        assert len(breed_moved) == 5, '4 fishes and one shark should have moved due to breeding'
        assert len(breed_moved) == (len(a_list) - 1)
        grid_df = grid.get_simulation_grid_data()
        assert len(grid_df[grid_df['animal_type'] == Animal.Shark]) == 2, 'Should be 2 Sharks'
        assert len(grid_df[grid_df['animal_type'] == Animal.Fish]) == 8, 'Should be 8 fishes'

# AM: add some further tests. For the full testing we need all possible
# unit tests for all the modules. Let's add here just some obvious integration
# tests to get that overall behavior is as expected
    def test_empty(self):
        '''
        Tests that an empty grid throws an exception of no sharks
        '''
        client = SimulationClient('sqlite:///:memory:')
        grid = SimulationGrid(persistence=client, simulation_parameters=sim_config_empty)

        with pytest.raises(EndOfSimulatioError):
            grid._check_deads()

        with pytest.raises(EndOfSimulatioError):
            grid.play_turn()

    def test_fish_only(self):
        '''
        Test if non-empty, but fish only: the simulation should end
        Make two cases:
            (1) add manually the fish to empty grid
            (2) use the config with updated value for corresponding key
        '''
        # No. 1
        client = SimulationClient('sqlite:///:memory:')
        grid = SimulationGrid(persistence=client, simulation_parameters=sim_config_empty)
        a_list = [
            (Animal.Fish, SquareGridCoordinate(x=1, y=1)),
        ]

        for t, c in a_list:
            client.init_animal(sim_id=grid._sid, current_turn=0, animal_type=t, coordinate=c)

        with pytest.raises(EndOfSimulatioError):
            grid._check_deads()

        with pytest.raises(EndOfSimulatioError):
            grid.play_turn()

        # repeat the same for No. 2
        del client, grid, a_list

        sim_config_empty_local = copy.deepcopy(sim_config_empty)
        sim_config_empty_local['init_nb_fish'] = 1

        client = SimulationClient('sqlite:///:memory:')
        grid = SimulationGrid(persistence=client, simulation_parameters=sim_config_empty_local)
        with pytest.raises(EndOfSimulatioError):
            grid._check_deads()

        with pytest.raises(EndOfSimulatioError):
            grid.play_turn()

    def test_check_if_occupied(self):
        '''
        Test the modified function
        '''
        client = SimulationClient('sqlite:///:memory:')
        grid = SimulationGrid(persistence=client, simulation_parameters=sim_config_empty)
        a_list = [
            (Animal.Fish, SquareGridCoordinate(x=1, y=1)),
            (Animal.Shark, SquareGridCoordinate(x=2, y=2))
        ]
        # adding some food and a shark
        for t, c in a_list:
            client.init_animal(sim_id=grid._sid, current_turn=0, animal_type=t, coordinate=c)
        grid.animals = grid.get_simulation_grid_data()
        grid.occupied_coord = set(zip(grid.animals.coord_x, grid.animals.coord_y))
        assert grid.check_if_occupied(SquareGridCoordinate(x=1, y=1)) == True, 'should be occupied'
        assert grid.check_if_occupied(SquareGridCoordinate(x=2, y=2)) == True, 'should be occupied'
        assert grid.check_if_occupied(SquareGridCoordinate(x=1, y=2)) == False, 'should be spare'
        # repeat the same for 1x1 case with initialization of 1 Fish
        del client, grid, a_list
        sim_config_empty_local = copy.deepcopy(sim_config_empty)
        sim_config_empty_local['init_nb_fish'] = 1
        sim_config_empty_local['grid_size'] = 1
        client = SimulationClient('sqlite:///:memory:')
        grid = SimulationGrid(persistence=client, simulation_parameters=sim_config_empty_local)
        assert grid.check_if_occupied(SquareGridCoordinate(x=0, y=0)) == True, 'should be occupied'

    # AM: test idea: add another test for max_turns (for this need to add additional config parameter)
    def test_shark_only_max_turn(self):
        '''
        Test if max_turn parameter works as expected
        '''
        pass

    # AM: test
    def test_topology_agnostic_eating(self):
        '''
        Check the topology-agnostic behavior, i.e. that the shark from one corner
        will eat the fish from the opposite corner
        '''
        # set fish breeding probability to 0
        sim_config_empty_local = copy.deepcopy(sim_config_empty)
        sim_config_empty_local['fish_breed_probability'] = 0
        client = SimulationClient('sqlite:///:memory:')
        grid = SimulationGrid(persistence=client, simulation_parameters=sim_config_empty_local)
        a_list = [
            (Animal.Fish, SquareGridCoordinate(x=0, y=0)),
            (Animal.Shark, SquareGridCoordinate(x=9, y=9))
        ]
        # adding some food and a shark
        for t, c in a_list:
            client.init_animal(sim_id=grid._sid, current_turn=0, animal_type=t, coordinate=c)
        # updating grid turn
        grid._sim_turn = 6
        # AM: the latest two rows to allow optimized 'occupied coordinates' check
        grid.animals = grid.get_simulation_grid_data()
        grid.occupied_coord = set(zip(grid.animals.coord_x, grid.animals.coord_y))
        shark_update = grid._eat()
        # shark has eaten
        shark = grid._persistence.get_animals_by_type(sim_id=grid._sid, animal_type=Animal.Shark).iloc[0]
        # the shark is in update list
        assert len(shark_update) == 1, 'There should be one shark in update list'
        assert shark.last_fed == 6, 'Shark last fed value should have updated'
        assert shark_update[shark.oid] == SquareGridCoordinate(x=9, y=9), 'Shark previous coordinate in shark update'
        # Fish is dead
        animal_in_square = client.get_animal_in_position(sim_id=grid._sid,
                                                         coordinate=SquareGridCoordinate(int(shark.coord_x),
                                                                                         int(shark.coord_y)),
                                                         live_only=False)
        assert len(animal_in_square) == 2, 'there shoud be two animals in that square'
        # check that there is no fish
        fish = grid._persistence.get_animals_by_type(sim_id=grid._sid, animal_type=Animal.Fish)
        assert len(fish) == 0, 'there should be no fish alive' # there is no fish (since we put 0 breeding probability)


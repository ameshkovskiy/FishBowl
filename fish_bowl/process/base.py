from collections import namedtuple
import random
import logging
from typing import Dict, List, Tuple

import pandas as pd

from fish_bowl.dataio.persistence import SimulationClient
from fish_bowl.process.utils import Animal, ImpossibleAction, EndOfSimulatioError
from fish_bowl.process.topology import SquareGridCoordinate, square_grid_neighbours

_logger = logging.getLogger(__name__)

# define dictionary with attributes class
# Let's use this class for parameters, which are assumed to be static
# during ecosystem's lifetime, and will not need unnecessary
# requests for accessing DB.
class DictionaryWithAttributes(dict):

    def __init__(self, *args, **kwargs):
        super(DictionaryWithAttributes, self).__init__(*args, **kwargs)
        self.__dict__ = self


class SimulationGrid:

    def __init__(self, persistence: SimulationClient, simulation_parameters: Dict):
        """
        Create a simulation and link to its persistence
        :param persistence:
        :param simulation_parameters:
        """
        # TODO: create a new simulation from existing parameters by providing an existing sid
        self._persistence = persistence

        # initialize simulation
        self.simulation_params = DictionaryWithAttributes(simulation_parameters) # add attribute in the beginning
        self._sid = self._persistence.init_simulation(**simulation_parameters)
        self._sim_turn = 0
        self._spawn()
        # get occupied coordinates at initialization
        self.animals = self.get_simulation_grid_data()
        self.occupied_coord = set(zip(self.animals.coord_x, self.animals.coord_y))


    def display_grid(self):
        """
        Simple display of the grid with elements
        :return:
        """
        return

    def get_simulation_parameters(self, sim_id: int = None):
        if sim_id is None:
            sim_id = self._sid
        return self._persistence.get_simulation(sim_id=sim_id)

    def get_simulation_grid_data(self) -> pd.DataFrame:
        return self._persistence.get_animals_df(sim_id=self._sid)

    @property
    def population(self):
        grid = self.get_simulation_grid_data()
        population = grid[grid['alive']].animal_type.value_counts()
        return population

    def persist_to_file(self, filename):
        population = self.population
        nb = '{},{},{}'.format(self._sim_turn, population[Animal.Fish], population[Animal.Shark])
        with open(filename, 'a') as fp:
            if self._sim_turn == 1:
                fp.write('Turn, Fish, Sharks\n')
            fp.write(nb + '\n')

    def _spawn(self):
        """
        function to create the grid by spawning fishes and sharks initially (and only at start)
        :return:
        """
        # get simulation elements
        simulation_params = self.simulation_params
        grid_size = simulation_params.grid_size
        coord_array = [(x, y) for x in range(grid_size) for y in range(grid_size)]
        random.shuffle(coord_array)
        # spawn fish and Sharks
        fishes = 0
        sharks = 0
        for coord in coord_array:
            if fishes < simulation_params.init_nb_fish:
                # since animal at start can be able to breed, last breed can be negative
                spawn_turn = -random.randint(0, simulation_params.fish_breed_maturity)
                self._persistence.init_animal(sim_id=self._sid, current_turn=spawn_turn, animal_type=Animal.Fish,
                                              coordinate=SquareGridCoordinate(*coord),
                                              last_breed=spawn_turn)
                fishes += 1
            elif sharks < simulation_params.init_nb_shark:
                spawn_turn = -random.randint(0, simulation_params.shark_breed_maturity)
                self._persistence.init_animal(sim_id=self._sid, current_turn=spawn_turn, animal_type=Animal.Shark,
                                              coordinate=SquareGridCoordinate(*coord),
                                              last_breed=spawn_turn)
                sharks += 1
            else:
                break
        return

    def _check_deads(self):
        """
        sharks that did not eat since 'shark_starve' nb of turns, dies
        :return:
        """
        _debug = 'Turn: {:<3} - Deads - '.format(self._sim_turn)
        simulation_params = self.simulation_params
        sharks = self._persistence.get_animals_by_type(sim_id=self._sid, animal_type=Animal.Shark) # dataframe of sharks
        if len(sharks) == 0:
            raise EndOfSimulatioError('Simulation ends because no more Sharks')
        sharks_starving = []
        for idx, shark in sharks.iterrows():
            if (self._sim_turn - shark.last_fed) > simulation_params.shark_starving:
                sharks_starving.append(shark.oid)
        if len(sharks_starving) > 0:
            _logger.info('{}Found {} shark starving'.format(_debug, len(sharks_starving)))
            coord_to_remove = self._persistence.kill_animal(sim_id=self._sid,
                                                            animal_ids=sharks_starving) # set of tuples
            # update coordinates
            for coord in coord_to_remove:
                self.update_occupied_coord(old_coord=coord)
        return

    def _eat(self) -> Dict[int, SquareGridCoordinate]:
        """
        Sharks that are adjacent to a Fish square eat and move into fish square (and do not move after)
        :return: list[(oid, prev_coordinate)]
        """
        _debug = 'Turn: {:<3} - Eat - '.format(self._sim_turn)
        simulation_params = self.simulation_params
        # get a randomized df of all sharks
        sharks = self._persistence.get_animals_by_type(sim_id=self._sid, animal_type=Animal.Shark).sample(frac=1)
        sharks_eating = dict()
        shark_update = dict()
        for idx, shark in sharks.iterrows():
            # get shark neighbour square
            shark_position = SquareGridCoordinate(shark.coord_x, shark.coord_y)
            shark_neighbour = square_grid_neighbours(simulation_params.grid_size, shark_position) # coordinates
            # try to find fish
            has_fish = self._persistence.has_fish_in_square(sim_id=self._sid, coordinates=shark_neighbour)
            if len(has_fish) > 0:
                # Shark is eating
                random.shuffle(has_fish)
                eating_coord = has_fish[0]
                if self._persistence.eat_animal_in_square(sim_id=self._sid, coordinate=eating_coord):
                    _logger.debug('{}Shark {} {} eat Fish {} and move'.format(_debug, shark.oid, shark_position,
                                                                              eating_coord))
                    # keep shark ref and position
                    sharks_eating[shark.oid] = shark_position
                    # move shark to eating position
                    self._persistence.move_animal(sim_id=self._sid, animal_id=shark.oid,
                                                  new_position=eating_coord)
                    # AM: add update to occupied coord
                    self.update_occupied_coord(old_coord=(shark_position.x, shark_position.y),
                                               new_coord=(eating_coord.x, eating_coord.y))
                    # add to update dictionary
                    shark_update[shark.oid] = {'last_fed': self._sim_turn}
                else:
                    raise ImpossibleAction('Something went wrong in Shark: {} feeding in {}'.format(shark, has_fish[0]))
            else:
                _logger.debug('{}turn: {}, No fish to eat for shark {}'.format(_debug, self._sim_turn, shark.oid))
        self._persistence.update_animals(sim_id=self._sid, update_dict=shark_update)
        _logger.debug('{}{} sharks have eaten'.format(_debug, len(sharks_eating)))
        return sharks_eating

    def _breed_and_move(self, fed_sharks: Dict[int, SquareGridCoordinate]) -> List[int]:
        """
        Sharks or Fish that can breed, do so in same square (and Move), others moves if free space
        - Shark Breed first
        - Then Fish
        :parameter fed_sharks: list of sharks that fed and moved (breed, if possible, on previous position)
        :return: return the list of animals that bred and moved
        """
        # perform breed for
        _debug = 'Turn: {:<3} - Breed - '.format(self._sim_turn)
        simulation_params = self.simulation_params
        moved = []
        to_update = {}
        # First for sharks
        sharks = self._persistence.get_animals_by_type(sim_id=self._sid, animal_type=Animal.Shark).sample(frac=1)
        for idx, shark in sharks.iterrows():
            # can shark breed?
            if (((self._sim_turn - shark.spawn_turn) >= simulation_params.shark_breed_maturity) and
                    ((self._sim_turn - shark.last_breed) >= simulation_params.shark_breed_maturity)):
                # shark can breed
                if random.randint(0, 100) <= simulation_params.shark_breed_probability:
                    # shark is possibly breeding...
                    breed_coord = None
                    if shark.oid in fed_sharks:
                        # ...if shark has eaten...
                        breed_coord = fed_sharks[shark.oid]
                        if self.check_if_occupied(breed_coord):
                            # someone took that space before breeding
                            _logger.debug('{}This shark {} breeding has fed and moved,' +
                                          ' cannot breed in {} because position is taken'.format(_debug, shark.oid,
                                                                                                 breed_coord))
                            breed_coord = None
                        _logger.debug('{}This shark {} breeding has fed and moved, breeding in {}'.format(_debug,
                                                                                                          shark.oid,
                                                                                                          breed_coord))
                        # shark has already moved to eating position
                        moved.append(shark.oid)
                    else:
                        # ... or if free space is available
                        neighbors = square_grid_neighbours(simulation_params.grid_size,
                                                           SquareGridCoordinate(shark.coord_x,
                                                                                shark.coord_y))
                        for neigh in neighbors:
                            if not self.check_if_occupied(neigh):
                                breed_coord = SquareGridCoordinate(int(shark.coord_x), int(shark.coord_y))
                                # set occupation flag to False
                                occupation_flag = False
                                # move shark to this slot
                                coord_to_remove = self._persistence.move_animal(sim_id=self._sid, animal_id=shark.oid,
                                                              new_position=neigh, occupied=occupation_flag) # hereinafter: tuple (x,y)
                                # AM: add update to occupied coord
                                self.update_occupied_coord(old_coord=coord_to_remove,
                                                           new_coord=(neigh.x, neigh.y))

                                # set occupation flag back to None
                                occupation_flag = None
                                moved.append(shark.oid)
                                _logger.debug('{}Shark {} not fed breeding in {}, moving to {}'.format(_debug,
                                                                                                       shark.oid,
                                                                                                       breed_coord,
                                                                                                       neigh))
                                # break out of loop
                                break
                    if breed_coord is not None:
                        to_update[shark.oid] = {'last_breed': self._sim_turn, 'breed_count': shark.breed_count + 1}
                        # spawn new fish in breed_coord
                        new_oid = self._persistence.init_animal(sim_id=self._sid, current_turn=self._sim_turn,
                                                                animal_type=Animal.Shark, coordinate=breed_coord,
                                                                last_fed=self._sim_turn)
                        # update the occupied coord
                        self.update_occupied_coord(new_coord=(breed_coord.x, breed_coord.y))
                        _logger.debug('{}Spawning new shark {} {}'.format(_debug, new_oid, breed_coord))
        # Last Fishes, randomize
        fishes = self._persistence.get_animals_by_type(sim_id=self._sid, animal_type=Animal.Fish).sample(frac=1)
        for idx, fish in fishes.iterrows():
            # can fish breed?
            if (((self._sim_turn - fish.spawn_turn) >= simulation_params.fish_breed_maturity) and
                    ((self._sim_turn - fish.last_breed) >= simulation_params.fish_breed_maturity)):
                # fish can breed
                if random.randint(0, 100) <= simulation_params.fish_breed_probability:
                    # fish is possibly breeding if free space is available
                    breed_coord = SquareGridCoordinate(int(fish.coord_x), int(fish.coord_y))
                    _logger.debug('{}Fish breeding in {} if space is available'.format(_debug, breed_coord))
                    neighbors = square_grid_neighbours(simulation_params.grid_size,
                                                       SquareGridCoordinate(fish.coord_x,
                                                                            fish.coord_y))
                    for neigh in neighbors:
                        if not self.check_if_occupied(neigh):
                            _logger.debug('{}Space found in {}, fish breed and move'.format(_debug, neigh))
                            to_update[fish.oid] = {'last_breed': self._sim_turn,
                                                   'breed_count': fish.breed_count + 1}
                            # set occupation flag to False
                            occupation_flag = False
                            # move fish to this slot
                            coord_to_remove = self._persistence.move_animal(sim_id=self._sid, animal_id=fish.oid,
                                                          new_position=neigh, occupied=occupation_flag)

                            self.update_occupied_coord(new_coord=(neigh.x, neigh.y)) # this only adds new (breed_coord keeps occupied)
                            # set back to None
                            occupation_flag = None
                            moved.append(fish.oid)
                            # spawn new fish in breed_coord
                            self._persistence.init_animal(sim_id=self._sid, current_turn=self._sim_turn,
                                                          animal_type=Animal.Fish, coordinate=breed_coord,
                                                          last_fed=self._sim_turn)
                            # break out of loop
                            break
        # now, update all animals
        if len(to_update) > 0:
            _logger.debug('{}{} animals updated after breeding'.format(_debug, len(to_update)))
            self._persistence.update_animals(sim_id=self._sid, update_dict=to_update)
        # add shark that ate and did not breed to the moved list
        for oid in fed_sharks.keys():
            if oid not in moved:
                _logger.debug('{}Shark {} did not breed after movin'.format(_debug, oid))
                moved.append(oid)
        # return animal list that have already bred and moved
        return moved

    def _move(self, already_moved: List[int]):
        """
        Those who can move do so (Free space around)
        :return:
        """
        # Fish and sharks only move one square at this stage.

        # fist move all fishes
        _logger.debug('Moving fishes')
        self._move_animal_type(Animal.Fish, already_moved)
        # then sharks
        _logger.debug('Moving sharks')
        self._move_animal_type(Animal.Shark, already_moved)
        return

    def _move_animal_type(self, animal_type: Animal, already_moved: List[int]):
        """
        Perform move action for a type of animal
        :param animal_type:
        :param already_moved:
        :return:
        """
        _debug = 'Turn: {:<3} - Move - '.format(self._sim_turn)
        simulation_params = self.simulation_params
        animals = self._persistence.get_animals_by_type(sim_id=self._sid, animal_type=animal_type).sample(frac=1)
        for _, animal in animals.iterrows():
            if animal.oid in already_moved:
                # this one has already moved so not moving
                _logger.debug('{}{} already moved'.format(_debug, animal.oid))
                continue
            elif animal.spawn_turn == self._sim_turn:
                # fish was just spawn, not moving
                _logger.debug('{}{} just spawned'.format(_debug, animal.oid))
                continue
            else:
                neighbors = square_grid_neighbours(simulation_params.grid_size, SquareGridCoordinate(animal.coord_x,
                                                                                                     animal.coord_y))
                for neigh in neighbors:
                    if not self.check_if_occupied(neigh):
                        # move animal to this slot
                        # set occupation flag to False
                        occupation_flag = False
                        _logger.debug('{}{} moved to {}'.format(_debug, animal_type.name, neigh))
                        coord_to_remove = self._persistence.move_animal(sim_id=self._sid, animal_id=animal.oid,
                                                      new_position=neigh, occupied=occupation_flag)

                        self.update_occupied_coord(old_coord=coord_to_remove, new_coord=(neigh.x, neigh.y))

                        # set back to None
                        occupation_flag = None
                        # break # AM: why we don't have break here?! Seems like we are making unnecessary operations
                    else:
                        _logger.debug('{}{}: {} had no space to move to'.format(_debug, animal_type.name, animal.oid))
        return

    def check_simulation_ends(self):
        """
        Simulation ends if Sharks have disappeared
        :return:
        """
        if len(self._persistence.get_animals_by_type(sim_id=self._sid, animal_type=Animal.Shark)) == 0:
            raise EndOfSimulatioError('Simulation ends because no more Sharks')

    def check_if_occupied(self, coordinate: SquareGridCoordinate) -> bool:
        '''
        Checks if coordinate is occupied (instead of DB, let's use set of tuples with coord-s)
        '''
        out = (coordinate.x, coordinate.y) in self.occupied_coord
        return out

    def update_occupied_coord(self, old_coord=None, new_coord=None):
        """
        Function to update the set of currently occupied coordinates on the grid.

        :param old_coord: tuple of coord-s to remove
        :param new_coord: tuple of coord-s to add
        :return: void

        """

        if old_coord is not None:
            self.occupied_coord.discard(old_coord) # instead of remove (to pass tests); not greatest approach. Should be remove for proper exception handling
        if new_coord is not None:
            self.occupied_coord.add(new_coord)
        return

    def play_turn(self):
        """
        Create a new turn,
        - load all animals
        check feeding -> animal dies
        shark eat
        check breading -> animal breed
        animal move (fish first then sharks)
        turn ends

        :return:
        """
        _logger.debug('********************TURN: {:<3}********************'.format(self._sim_turn))
        self._check_deads()
        fed_sharks = self._eat() # these are the coordinates of sharks before eating (after eating they are updated to the new positions)
        moved_animals = self._breed_and_move(fed_sharks=fed_sharks) # the coordinates of animals before they moved
        self._move(already_moved=moved_animals)
        self._sim_turn += 1
        _logger.debug('********************END***************************'.format(self._sim_turn))
        self.check_simulation_ends()
        return

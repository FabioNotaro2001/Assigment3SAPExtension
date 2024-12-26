package sap.ass2.users.application;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.Optional;
import java.util.stream.Collectors;

import io.vertx.core.json.JsonObject;
import sap.ass2.users.domain.RepositoryException;
import sap.ass2.users.domain.User;
import sap.ass2.users.domain.UserBuilder;
import sap.ass2.users.domain.UserEvent;
import sap.ass2.users.domain.UsersRepository;

public class UsersRepositoryImpl implements UsersRepository, UserEventsConsumer {
    private Path dbFile;
	Optional<Map<String, User>> users;

    public UsersRepositoryImpl(CustomKafkaListener listener) throws RepositoryException {
        this.dbFile = Path.of("./database/db.txt");  
		try {
			new File(this.dbFile.toString()).createNewFile();
		} catch (IOException e) {
			throw new RepositoryException();
		}

		this.users = Optional.empty();
		listener.onEach(this::consumeEvents);
    }

	/**
	 * // Converts the given user into a JSON object and then save it into the DB.
	 */
    @Override
    public void saveUserEvent(UserEvent event) throws RepositoryException {
		SpecialFileParser parser = new SpecialFileParser(this.dbFile);
		try {
			parser.addEvent(event);

			if (this.users.isEmpty()) {
				return;
			}
			var user = Optional.ofNullable(users.get().get(event.userId()));
			if (user.isEmpty()) {
				return;
			}
			user.get().applyEvent(event);
		} catch (IOException e) {
			throw new RepositoryException();
		}
    }

	private void loadUsers() throws RepositoryException {
		SpecialFileParser parser = new SpecialFileParser(this.dbFile);
		try {
			var userEvents = new ArrayList<>(parser.getUserEvents());
			var builders = userEvents.stream()
				.map(ev -> ev.userId())
				.distinct()
				.collect(Collectors.toMap(
					Function.identity(),
					id -> new UserBuilder()
				));
			userEvents.stream()
				.forEach(ev -> builders.get(ev.userId()).applyEvent(ev));
			this.users = Optional.of(builders.values().stream()
				.map(UserBuilder::build)
				.collect(Collectors.toMap(
					User::getId,
					Function.identity()
				)));
		} catch (IOException e) {
			throw new RepositoryException();
		}
	}

    @Override
    public List<User> getUsers() throws RepositoryException {
		if (users.isEmpty()) {
			loadUsers();
		}

		return this.users.get().values().stream().collect(ArrayList::new, ArrayList::add, ArrayList::addAll);
    }

    @Override
    public Optional<User> getUserByID(String id) throws RepositoryException {
		if (users.isEmpty()) {
			loadUsers();
		}

		return Optional.ofNullable(this.users.get().get(id));
    }

	@Override
	public void consumeEvents(String message) {
		JsonObject obj = new JsonObject(message);
		try {
			saveUserEvent(UserEvent.from(obj.getString("userId"), obj.getInteger("credits"), obj.getDouble("deltaX"), obj.getDouble("deltaY")));
		} catch (RepositoryException e) {
			e.printStackTrace();
			throw new RuntimeException(e);
		}
	}
}

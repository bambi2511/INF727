package fr.telecom.paristech;

	/**
	 * Constantes utilisées dans ce projet
	 */
public final class Constants {

        public static final String USERNAME = "flecerf";
        public static final String BASE_DIRECTORY = "/tmp/flecerf/";
        public static final String SPLITS_DIRECTORY = "/tmp/flecerf/split/";
        public static final String MAPS_DIRECTORY = "/tmp/flecerf/map/";
        public static final String SHUFFLE_DIRECTORY = "/tmp/flecerf/shuffle/";
        public static final String REDUCE_DIRECTORY = "/tmp/flecerf/reduce/";
        public static final String SLAVE_PATH = "/tmp/flecerf/slave.jar";
        public static final String SLAVES_PATH = "/tmp/flecerf/slaves.txt";

        /**
         * Not instantiable class
         */
        public Constants() {
                throw new UnsupportedOperationException();
        }

}

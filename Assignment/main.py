from tweetAPI import TwitterAPI


def main():
    # user and password information
    print("Connection Information")
    print("-" * 30)
    user = 'root'
    database = "Twitter"
    print(f"User: {user}\nConnecting to: {database}")
    password = input("Enter Password: ")
    connection = TwitterAPI(user, password, database)
    print("Connected Successfully")
    print("-" * 30)

    # batch insert
    print(connection.post_tweet())

    # get timeline - no dataframe (frame=False)
    print(connection.get_timeline(frame=False))

    # get followers from a user
    user_interest = 5
    print(connection.get_followers(user_interest, frame=False))

    # get followees
    print(connection.get_followees(user_interest, frame=False))

    # get tweet from a user
    print(connection.get_tweet(user_interest, frame=False))


if __name__ == "__main__":
    main()
